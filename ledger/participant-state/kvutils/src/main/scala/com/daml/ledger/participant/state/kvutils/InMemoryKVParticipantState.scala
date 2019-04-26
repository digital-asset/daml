package com.daml.ledger.participant.state.kvutils

import java.util.UUID

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Kill, Props}
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.SimpleString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.akkastreams.Dispatcher
import com.digitalasset.platform.akkastreams.SteppingMode.OneAfterAnother
import com.digitalasset.platform.services.time.TimeModel
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.collection.breakOut

object InMemoryKVParticipantState {

  /** The complete state of the participant at a given point in time.
    * This emulates a key-value blockchain with a log of commits and a key-value store.
    * The commit log provides the ordering for the log entries, and its height is used
    * as the [[Offset]].
    * */
  case class State(
      log: Vector[(DamlLogEntryId, DamlSubmission)],
      recordTime: Timestamp,
      // Store is the key-value store that the commits mutate.
      store: Map[ByteString, ByteString],
      config: Configuration
  )

  // Message sent to the commit actor to commit a submission to the state.
  case class CommitMessage(
      // Submitter chosen entry identifier
      entryId: DamlLogEntryId,
      submission: DamlSubmission
  )
}

/* Implementation of the participant-state using the key-value utilities and an in-memory map. */
class InMemoryKVParticipantState(implicit system: ActorSystem)
    extends ReadService
    with WriteService
    with AutoCloseable {
  import InMemoryKVParticipantState._

  val ledgerId = SimpleString.assertFromString(UUID.randomUUID.toString)

  // DAML Engine for transaction validation.
  private val engine = Engine()

  // Random number generator for generating unique entry identifiers.
  private val rng = new java.util.Random

  // Namespace prefix for log entries.
  private val NS_LOG_ENTRIES = ByteString.copyFromUtf8("L")

  // Namespace prefix for DAML state.
  private val NS_DAML_STATE = ByteString.copyFromUtf8("DS")

  // Reference to the latest immutable state. Reference is only updated by the CommitActor.
  // Reading from the state must happen by first taking the reference (val state = stateRef).
  @volatile private var stateRef: State =
    State(
      log = Vector.empty[(DamlLogEntryId, DamlSubmission)],
      recordTime = Timestamp.Epoch,
      store = Map.empty[ByteString, ByteString],
      config = Configuration(
        timeModel = TimeModel.reasonableDefault
      )
    )

  // Akka actor that receives submissions and commits them to the key-value store.
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  class CommitActor extends Actor {
    override def receive: Receive = {
      case CommitMessage(entryId, submission) =>
        val state = stateRef
        if (state.store.contains(entryId.getEntryId)) {
          // The entry identifier already in use, drop the message and let the
          // client retry submission.
        } else {
          // Process the submission to produce the log entry and the state updates.
          val (logEntry, damlStateUpdates) = KeyValueCommitting.processSubmission(
            engine,
            state.config,
            entryId,
            state.recordTime,
            submission,
            submission.getInputLogEntriesList.asScala
              .map(eid => eid -> getLogEntry(state, eid))(breakOut),
            submission.getInputDamlStateList.asScala
              .map(key => key -> getDamlState(state, key))(breakOut)
          )

          // Combine the abstract log entry and the state updates into concrete updates to the store.
          val allUpdates =
            damlStateUpdates.map {
              case (k, v) =>
                NS_DAML_STATE.concat(KeyValueCommitting.packDamlStateKey(k)) ->
                  KeyValueCommitting.packDamlStateValue(v)
            } + (entryId.getEntryId -> KeyValueCommitting.packDamlLogEntry(logEntry))

          // Atomically update the state reference, recording the commit
          // and the updated state.
          stateRef = state.copy(
            log = state.log :+ (entryId -> submission),
            store = state.store ++ allUpdates
          )

          // Wake up consumers.
          dispatcher.signalNewHead(stateRef.log.size)
        }
    }
  }
  private val commitActorRef =
    system.actorOf(Props(new CommitActor), s"commit-actor-${ledgerId.underlyingString}")

  // Dispatcher to subscribe to 'Update' events derived from the state.
  private val beginning: Int = 0
  private val dispatcher: Dispatcher[Int, Update] = Dispatcher(
    steppingMode = OneAfterAnother(
      (idx: Int, _) => idx + 1,
      (idx: Int) => Future.successful(getUpdate(idx, stateRef))
    ),
    zeroIndex = beginning,
    headAtInitialization = beginning
  )

  private def getUpdate(idx: Int, state: State): Update = {
    if (idx < 0 || idx >= state.log.size)
      sys.error(s"getUpdate: $idx out of bounds (${state.log.size})")

    // Resolve the "height" to log entry.
    val entryId = state.log(idx)._1

    state.store
      .get(entryId.getEntryId)
      .map { blob =>
        KeyValueConsumption.logEntryToUpdate(entryId, KeyValueConsumption.unpackDamlLogEntry(blob))
      }
      .getOrElse(
        sys.error(s"getUpdate: $entryId not found from store!")
      )
  }

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
    dispatcher
      .startingAt(beginAfter.map(_.components.head.toInt + 1).getOrElse(beginning))
      .map {
        case (idx, update) =>
          Offset(Array(idx.toLong)) -> update
      }
  }

  private def getLogEntry(state: State, entryId: DamlLogEntryId): DamlLogEntry =
    DamlLogEntry.parseFrom(state.store(entryId.getEntryId))

  private def getDamlState(state: State, key: DamlStateKey): Option[DamlStateValue] =
    state.store
      .get(NS_DAML_STATE.concat(KeyValueCommitting.packDamlStateKey(key)))
      .map(DamlStateValue.parseFrom)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Unit = {

    val submission =
      KeyValueSubmission.transactionToSubmission(submitterInfo, transactionMeta, transaction)

    // Submit the transaction to the committer.
    commitActorRef ! CommitMessage(
      allocateEntryId,
      submission
    )
  }

  // Back-channel for uploading DAML-LF archives.
  def uploadArchive(archive: Archive): Unit = {
    commitActorRef ! CommitMessage(
      allocateEntryId,
      KeyValueSubmission.archiveToSubmission(archive)
    )
  }

  /** Retrieve the static initial conditions of the ledger, containing
    * the ledger identifier and the epoch of the ledger record time.
    *
    * Returns a future since the implementation may need to first establish
    * connectivity to the underlying ledger. The implementer may assume that
    * this method is called only once, or very rarely.
    */
  // FIXME(JM): Add configuration to initial conditions!
  override def getLedgerInitialConditions(): Future[LedgerInitialConditions] =
    Future.successful(LedgerInitialConditions(ledgerId, Timestamp.Epoch))

  private def allocateEntryId(): DamlLogEntryId = {
    val nonce: Array[Byte] = Array.ofDim(32)
    rng.nextBytes(nonce)
    DamlLogEntryId.newBuilder
      .setEntryId(NS_LOG_ENTRIES.concat(ByteString.copyFrom(nonce)))
      .build
  }

  override def close(): Unit = {
    // FIXME(JM): We'll want to wait for termination!
    commitActorRef ! Kill
  }
}
