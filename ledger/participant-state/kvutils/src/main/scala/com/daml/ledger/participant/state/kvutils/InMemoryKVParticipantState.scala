// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Clock
import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Kill, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.digitalasset.platform.services.time.TimeModel
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object InMemoryKVParticipantState {

  /** The complete state of the ledger at a given point in time.
    * This emulates a key-value blockchain with a log of commits and a key-value store.
    * The commit log provides the ordering for the log entries, and its height is used
    * as the [[Offset]].
    * */
  case class State(
      // Log of commits, which are either [[DamlSubmission]]s or heartbeats.
      // Replaying the commits constructs the store.
      commitLog: Vector[Commit],
      // Current record time of the ledger.
      recordTime: Timestamp,
      // Store containing both the [[DamlLogEntry]] and [[DamlStateValue]]s.
      // The store is mutated by applying [[DamlSubmission]]s. The store can
      // be reconstructed from scratch by replaying [[State.commits]].
      store: Map[ByteString, ByteString],
      // Current ledger configuration.
      config: Configuration
  )

  sealed trait Commit extends Serializable with Product

  /** A commit sent to the [[InMemoryKVParticipantState.CommitActor]],
    * which inserts it into [[State.commitLog]].
    */
  final case class CommitSubmission(
      entryId: DamlLogEntryId,
      submission: DamlSubmission
  ) extends Commit

  /** A periodically emitted heartbeat that is committed to the ledger. */
  final case class CommitHeartbeat(recordTime: Timestamp) extends Commit

}

/** Implementation of the participant-state [[ReadService]] and [[WriteService]] using
  * the key-value utilities and an in-memory key-value store.
  *
  * This example uses Akka actors and streams.
  * See Akka documentation for information on them:
  * https://doc.akka.io/docs/akka/current/index-actors.html.
  */
class InMemoryKVParticipantState(implicit system: ActorSystem, mat: Materializer)
    extends ReadService
    with WriteService
    with AutoCloseable {

  import InMemoryKVParticipantState._

  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit val ec: ExecutionContext = mat.executionContext

  val ledgerId: LedgerString.T = Ref.LedgerString.assertFromString(UUID.randomUUID.toString)

  // The ledger configuration
  private val ledgerConfig = Configuration(timeModel = TimeModel.reasonableDefault)

  // DAML Engine for transaction validation.
  private val engine = Engine()

  // Random number generator for generating unique entry identifiers.
  private val rng = new java.util.Random

  // Namespace prefix for log entries.
  private val NS_LOG_ENTRIES = ByteString.copyFromUtf8("L")

  // Namespace prefix for DAML state.
  private val NS_DAML_STATE = ByteString.copyFromUtf8("DS")

  /** Interval for heartbeats. Heartbeats are committed to [[State.commitLog]]
    * and sent as [[Update.Heartbeat]] to [[stateUpdates]] consumers.
    */
  private val HEARTBEAT_INTERVAL = 5.seconds

  // Name of this participant, ultimately pass this info in command-line
  val participantId = "in-memory-participant"

  /** Reference to the latest state of the in-memory ledger.
    * This state is only updated by the [[CommitActor]], which processes submissions
    * sequentially and non-concurrently.
    *
    * Reading from the state must happen by first taking the reference (val state = stateRef),
    * as otherwise the reads may cross update boundaries.
    */
  @volatile private var stateRef: State =
    State(
      commitLog = Vector.empty[Commit],
      recordTime = Timestamp.Epoch,
      store = Map.empty[ByteString, ByteString],
      config = Configuration(
        timeModel = TimeModel.reasonableDefault
      )
    )

  /** Akka actor that receives submissions sequentially and
    * commits them one after another to the state, e.g. appending
    * a new ledger commit entry, and applying it to the key-value store.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  class CommitActor extends Actor {

    override def receive: Receive = {
      case commit @ CommitHeartbeat(newRecordTime) =>
        logger.trace(s"CommitActor: committing heartbeat, recordTime=$newRecordTime")
        // Update the state.
        stateRef = stateRef.copy(
          commitLog = stateRef.commitLog :+ commit,
          recordTime = newRecordTime
        )
        // Wake up consumers.
        dispatcher.signalNewHead(stateRef.commitLog.size)

      case commit @ CommitSubmission(entryId, submission) =>
        val state = stateRef
        val newRecordTime = getNewRecordTime

        if (state.store.contains(entryId.getEntryId)) {
          // The entry identifier already in use, drop the message and let the
          // client retry submission.
          logger.warn(s"CommitActor: duplicate entry identifier in commit message, ignoring.")
        } else {
          logger.trace(
            s"CommitActor: processing submission ${KeyValueCommitting.prettyEntryId(entryId)}...")
          // Process the submission to produce the log entry and the state updates.
          val (logEntry, damlStateUpdates) = KeyValueCommitting.processSubmission(
            engine,
            state.config,
            entryId,
            newRecordTime,
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

          logger.trace(
            s"CommitActor: committing ${KeyValueCommitting.prettyEntryId(entryId)} and ${allUpdates.size} updates to store.")

          // Update the state.
          stateRef = state.copy(
            recordTime = newRecordTime,
            commitLog = state.commitLog :+ commit,
            store = state.store ++ allUpdates
          )

          // Wake up consumers.
          dispatcher.signalNewHead(stateRef.commitLog.size)
        }
    }
  }

  /** Instance of the [[CommitActor]] to which we send messages. */
  private val commitActorRef = {
    // Start the commit actor.
    val actorRef =
      system.actorOf(Props(new CommitActor), s"commit-actor-$ledgerId")

    // Schedule heartbeat messages to be delivered to the commit actor.
    // This source stops when the actor dies.
    val _ = Source
      .tick(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL, ())
      .map(_ => CommitHeartbeat(getNewRecordTime))
      .to(Sink.actorRef(actorRef, onCompleteMessage = ()))
      .run()

    actorRef
  }

  /** The index of the beginning of the commit log */
  private val beginning: Int = 0

  /** Dispatcher to subscribe to 'Update' events derived from the state.
    * The index we use here is the "height" of the [[State.commitLog]].
    * This index is transformed into [[Offset]] in [[getUpdate]].
    *
    * [[Dispatcher]] is an utility written by Digital Asset implementing a fanout
    * for a stream of events. It is initialized with an initial offset and a method for
    * retrieving an event given an offset. It provides the method
    * [[Dispatcher.startingAt]] to subscribe to the stream of events from a
    * given offset, and the method [[Dispatcher.signalNewHead]] to signal that
    * new elements has been added.
    */
  private val dispatcher: Dispatcher[Int, Update] = Dispatcher(
    steppingMode = OneAfterAnother(
      (idx: Int, _) => idx + 1,
      (idx: Int) => Future.successful(getUpdate(idx, stateRef))
    ),
    zeroIndex = beginning,
    headAtInitialization = beginning
  )

  /** Helper for [[dispatcher]] to fetch [[DamlLogEntry]] from the
    * state and convert it into [[Update]].
    */
  private def getUpdate(idx: Int, state: State): Update = {
    assert(idx >= 0 && idx < state.commitLog.size)

    state.commitLog(idx) match {
      case CommitSubmission(entryId, _) =>
        state.store
          .get(entryId.getEntryId)
          .map { blob =>
            KeyValueConsumption.logEntryToUpdate(
              entryId,
              KeyValueConsumption.unpackDamlLogEntry(blob))
          }
          .getOrElse(
            sys.error(
              s"getUpdate: ${KeyValueCommitting.prettyEntryId(entryId)} not found from store!")
          )

      case CommitHeartbeat(recordTime) =>
        Update.Heartbeat(recordTime)
    }
  }

  /** Subscribe to updates to the participant state.
    * Implemented using the [[Dispatcher]] helper which handles the signalling
    * and fetching of entries from the state.
    *
    * See [[ReadService.stateUpdates]] for full documentation for the properties
    * of this method.
    */
  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
    dispatcher
      .startingAt(
        beginAfter
          .map(_.components.head.toInt + 1) // startingAt is inclusive, so jump over one element.
          .getOrElse(beginning)
      )
      .map {
        case (idx, update) =>
          Offset(Array(idx.toLong)) -> update
      }
  }

  /** Submit a transaction to the ledger.
    *
    * @param submitterInfo   : the information provided by the submitter for
    *                        correlating this submission with its acceptance or rejection on the
    *                        associated [[ReadService]].
    * @param transactionMeta : the meta-data accessible to all consumers of the
    *   transaction. See [[TransactionMeta]] for more information.
    * @param transaction     : the submitted transaction. This transaction can
    *                        contain contract-ids that are relative to this transaction itself.
    *                        These are used to refer to contracts created in the transaction
    *   itself. The participant state implementation is expected to convert
    *                        these into absolute contract-ids that are guaranteed to be unique.
    *                        This typically happens after a transaction has been assigned a
    *                        globally unique id, as then the contract-ids can be derived from that
    *                        transaction id.
    *
    *                        See [[WriteService.submitTransaction]] for full documentation for the properties
    *                        of this method.
    */
  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): CompletionStage[SubmissionResult] =
    CompletableFuture.completedFuture({
      // Construct a [[DamlSubmission]] message using the key-value utilities.
      // [[DamlSubmission]] contains the serialized transaction and metadata such as
      // the input contracts and other state required to validate the transaction.
      val submission =
        KeyValueSubmission.transactionToSubmission(submitterInfo, transactionMeta, transaction)

      // Send the [[DamlSubmission]] to the commit actor. The messages are
      // queued and the actor's receive method is invoked sequentially with
      // each message, hence this is safe under concurrency.
      commitActorRef ! CommitSubmission(
        allocateEntryId,
        submission
      )
      SubmissionResult.Acknowledged
    })

  /** Allocate a party on the ledger */
  override def allocateParty(
      hint: Option[String],
      displayName: Option[String]): CompletionStage[PartyAllocationResult] =
    // TODO: Implement party management
    CompletableFuture.completedFuture(PartyAllocationResult.NotSupported)

  /** Upload DAML-LF packages to the ledger.
    */
  override def uploadPublicPackages(
      archives: List[Archive],
      sourceDescription: String): CompletionStage[SubmissionResult] =
    CompletableFuture.completedFuture({
      commitActorRef ! CommitSubmission(
        allocateEntryId,
        KeyValueSubmission.archivesToSubmission(archives, sourceDescription, participantId)
      )
      SubmissionResult.Acknowledged
    })

  /** Retrieve the static initial conditions of the ledger, containing
    * the ledger identifier and the initial ledger record time.
    *
    * Returns a future since the implementation may need to first establish
    * connectivity to the underlying ledger. The implementer may assume that
    * this method is called only once, or very rarely.
    */
  // FIXME(JM): Add configuration to initial conditions!
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(initialConditions)

  /** Shutdown by killing the [[CommitActor]]. */
  override def close(): Unit = {
    commitActorRef ! Kill
  }

  private def getLogEntry(state: State, entryId: DamlLogEntryId): DamlLogEntry = {
    DamlLogEntry
      .parseFrom(
        state.store
          .getOrElse(
            entryId.getEntryId,
            sys.error(s"getLogEntry: Cannot find ${KeyValueCommitting.prettyEntryId(entryId)}!")
          )
      )
  }

  private def getDamlState(state: State, key: DamlStateKey): Option[DamlStateValue] =
    state.store
      .get(NS_DAML_STATE.concat(KeyValueCommitting.packDamlStateKey(key)))
      .map(DamlStateValue.parseFrom)

  private def allocateEntryId(): DamlLogEntryId = {
    val nonce: Array[Byte] = Array.ofDim(8)
    rng.nextBytes(nonce)
    DamlLogEntryId.newBuilder
      .setEntryId(NS_LOG_ENTRIES.concat(ByteString.copyFrom(nonce)))
      .build
  }

  /** The initial conditions of the ledger. The initial record time is the instant
    * at which this class has been instantiated.
    */
  private val initialConditions = LedgerInitialConditions(ledgerId, ledgerConfig, getNewRecordTime)

  /** Get a new record time for the ledger from the system clock.
    * Public for use from integration tests.
    */
  def getNewRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

}
