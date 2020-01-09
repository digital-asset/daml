// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io._
import java.time.Clock
import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.pattern.gracefulStop
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.{DamlKvutils => Proto}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

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
      store: Map[ByteString, ByteString]
  )

  object State {
    def empty = State(
      commitLog = Vector.empty[Commit],
      recordTime = Timestamp.Epoch,
      store = Map.empty[ByteString, ByteString]
    )

  }

  sealed trait Commit extends Serializable with Product

  /** A commit sent to the [[InMemoryKVParticipantState.CommitActor]],
    * which inserts it into [[State.commitLog]].
    */
  final case class CommitSubmission(
      entryId: Proto.DamlLogEntryId,
      envelope: ByteString
  ) extends Commit

  /** A periodically emitted heartbeat that is committed to the ledger. */
  final case class CommitHeartbeat(recordTime: Timestamp) extends Commit

  final case class AddPotentialResponse(idx: Int)

}

/** Implementation of the participant-state [[ReadService]] and [[WriteService]] using
  * the key-value utilities and an in-memory key-value store.
  *
  * This example uses Akka actors and streams.
  * See Akka documentation for information on them:
  * https://doc.akka.io/docs/akka/current/index-actors.html.
  */
class InMemoryKVParticipantState(
    val participantId: ParticipantId,
    val ledgerId: LedgerString = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    file: Option[File] = None,
)(implicit system: ActorSystem, mat: Materializer)
    extends ReadService
    with WriteService
    with AutoCloseable {

  import InMemoryKVParticipantState._

  private val logger = LoggerFactory.getLogger(this.getClass)

  private implicit val ec: ExecutionContext = mat.executionContext

  // The initial ledger configuration
  private val initialLedgerConfig = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault
  )

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

  /** Reference to the latest state of the in-memory ledger.
    * This state is only updated by the [[CommitActor]], which processes submissions
    * sequentially and non-concurrently.
    *
    * Reading from the state must happen by first taking the reference (val state = stateRef),
    * as otherwise the reads may cross update boundaries.
    */
  @volatile private var stateRef: State = {
    val initState = Try(file.map { f =>
      val is = new ObjectInputStream(new FileInputStream(f))
      val state = is.readObject().asInstanceOf[State]
      is.close()
      state
    }).toOption.flatten.getOrElse(State.empty)
    logger.info(s"Starting ledger backend at offset ${initState.commitLog.size}")
    initState
  }

  private def updateState(newState: State): Unit = {
    file.foreach { f =>
      val os = new ObjectOutputStream(new FileOutputStream(f))
      os.writeObject(newState)
      os.flush()
      os.close()
    }
    stateRef = newState
  }

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
        updateState(
          stateRef.copy(
            commitLog = stateRef.commitLog :+ commit,
            recordTime = newRecordTime
          ))
        // Wake up consumers.
        dispatcher.signalNewHead(stateRef.commitLog.size)

      case commit @ CommitSubmission(entryId, envelope) =>
        val submission: Proto.DamlSubmission = Envelope.open(envelope) match {
          case Left(err) => sys.error(s"Cannot open submission envelope: $err")
          case Right(Envelope.SubmissionMessage(submission)) => submission
          case Right(_) => sys.error("Unexpected message in envelope")
        }
        val state = stateRef
        val newRecordTime = getNewRecordTime

        if (state.store.contains(entryId.getEntryId)) {
          // The entry identifier already in use, drop the message and let the
          // client retry submission.
          logger.warn(s"CommitActor: duplicate entry identifier in commit message, ignoring.")
        } else {
          logger.trace(s"CommitActor: processing submission ${Pretty.prettyEntryId(entryId)}...")
          // Process the submission to produce the log entry and the state updates.

          val stateInputs: Map[Proto.DamlStateKey, Option[Proto.DamlStateValue]] =
            submission.getInputDamlStateList.asScala
              .map(key => key -> getDamlState(state, key))(breakOut)

          val (logEntry, damlStateUpdates) =
            KeyValueCommitting.processSubmission(
              engine,
              entryId,
              newRecordTime,
              initialLedgerConfig,
              submission,
              participantId,
              stateInputs
            )

          // Verify that the state updates match the pre-declared outputs.
          val expectedStateUpdates = KeyValueCommitting.submissionOutputs(entryId, submission)
          if (!(damlStateUpdates.keySet subsetOf expectedStateUpdates)) {
            sys.error(
              s"CommitActor: State updates not a subset of expected updates! Keys [${damlStateUpdates.keySet diff expectedStateUpdates}] are unaccounted for!")
          }

          // Combine the abstract log entry and the state updates into concrete updates to the store.
          val allUpdates =
            damlStateUpdates.map {
              case (k, v) =>
                NS_DAML_STATE.concat(KeyValueCommitting.packDamlStateKey(k)) ->
                  Envelope.enclose(v)
            } + (entryId.getEntryId -> Envelope.enclose(logEntry))

          logger.trace(
            s"CommitActor: committing ${Pretty.prettyEntryId(entryId)} and ${allUpdates.size} updates to store.")

          // Update the state.
          updateState(
            state.copy(
              recordTime = newRecordTime,
              commitLog = state.commitLog :+ commit,
              store = state.store ++ allUpdates
            ))

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
      .to(Sink.actorRef(actorRef, onCompleteMessage = (), onFailureMessage = (_) => ()))
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
  private val dispatcher: Dispatcher[Int] =
    Dispatcher(
      "inmemory-kv-participant-state",
      zeroIndex = beginning,
      headAtInitialization = beginning)

  /** Helper for [[dispatcher]] to fetch [[com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry]] from the
    * state and convert it into [[com.daml.ledger.participant.state.v1.Update]].
    */
  private def getUpdate(idx: Int, state: State): List[Update] = {
    assert(idx >= 0 && idx < state.commitLog.size)

    state.commitLog(idx) match {
      case CommitSubmission(entryId, _) =>
        state.store
          .get(entryId.getEntryId)
          .map { blob =>
            val logEntry = Envelope.open(blob) match {
              case Left(err) => sys.error(s"getUpdate: cannot open envelope: $err")
              case Right(Envelope.LogEntryMessage(logEntry)) => logEntry
              case Right(_) => sys.error(s"getUpdate: Envelope did not contain log entry")
            }
            KeyValueConsumption.logEntryToUpdate(entryId, logEntry)
          }
          .getOrElse(
            sys.error(s"getUpdate: ${Pretty.prettyEntryId(entryId)} not found from store!")
          )

      case CommitHeartbeat(recordTime) =>
        List(Update.Heartbeat(recordTime))
    }
  }

  override def currentHealth(): HealthStatus = Healthy

  /** Subscribe to updates to the participant state.
    * Implemented using the [[Dispatcher]] helper which handles the signalling
    * and fetching of entries from the state.
    *
    * See [[ReadService.stateUpdates]] for full documentation for the properties
    * of this method.
    */
  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    dispatcher
      .startingAt(
        beginAfter
          .map(_.components.head.toInt)
          .getOrElse(beginning),
        OneAfterAnother[Int, List[Update]](
          (idx: Int, _) => idx + 1,
          (idx: Int) => Future.successful(getUpdate(idx, stateRef))
        )
      )
      .collect {
        case (offset, updates) =>
          updates.zipWithIndex.map {
            case (el, idx) => Offset(Array(offset.toLong, idx.toLong)) -> el
          }
      }
      .mapConcat(identity)
      .filter {
        case (offset, _) =>
          if (beginAfter.isDefined)
            offset > beginAfter.get
          else true
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
        Envelope.enclose(
          submission
        )
      )
      SubmissionResult.Acknowledged
    })

  /** Allocate a party on the ledger */
  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId): CompletionStage[SubmissionResult] = {
    val party = hint.getOrElse(generateRandomParty())
    val submission =
      KeyValueSubmission.partyToSubmission(submissionId, Some(party), displayName, participantId)

    CompletableFuture.completedFuture({
      commitActorRef ! CommitSubmission(
        allocateEntryId,
        Envelope.enclose(
          submission
        )
      )
      SubmissionResult.Acknowledged
    })
  }

  private def generateRandomParty(): Ref.Party =
    Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString.take(8)}")

  /*
  /** Upload DAML-LF packages to the ledger */

   */

  /** Upload a collection of DAML-LF packages to the ledger. */
  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String]): CompletionStage[SubmissionResult] =
    CompletableFuture.completedFuture({
      commitActorRef ! CommitSubmission(
        allocateEntryId,
        Envelope.enclose(
          KeyValueSubmission
            .archivesToSubmission(
              submissionId,
              archives,
              sourceDescription.getOrElse(""),
              participantId))
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
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(initialConditions)

  /** Shutdown the in-memory participant state. */
  override def close(): Unit = {
    val _ = Await.ready(gracefulStop(commitActorRef, 5.seconds, PoisonPill), 6.seconds)
  }

  private def getDamlState(state: State, key: Proto.DamlStateKey): Option[Proto.DamlStateValue] =
    state.store
      .get(NS_DAML_STATE.concat(KeyValueCommitting.packDamlStateKey(key)))
      .map(blob =>
        Envelope.open(blob) match {
          case Right(Envelope.StateValueMessage(v)) => v
          case _ => sys.error(s"getDamlState: Envelope did not contain a state value")
      })

  private def allocateEntryId: Proto.DamlLogEntryId = {
    val nonce: Array[Byte] = Array.ofDim(8)
    rng.nextBytes(nonce)
    Proto.DamlLogEntryId.newBuilder
      .setEntryId(NS_LOG_ENTRIES.concat(ByteString.copyFrom(nonce)))
      .build
  }

  /** The initial conditions of the ledger. The initial record time is the instant
    * at which this class has been instantiated.
    */
  private val initialConditions =
    LedgerInitialConditions(ledgerId, initialLedgerConfig, getNewRecordTime)

  /** Get a new record time for the ledger from the system clock.
    * Public for use from integration tests.
    */
  def getNewRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  /** Submit a new configuration to the ledger. */
  override def submitConfiguration(
      maxRecordTime: Timestamp,
      submissionId: SubmissionId,
      config: Configuration): CompletionStage[SubmissionResult] =
    CompletableFuture.completedFuture({
      val submission =
        KeyValueSubmission
          .configurationToSubmission(maxRecordTime, submissionId, participantId, config)
      commitActorRef ! CommitSubmission(
        allocateEntryId,
        Envelope.enclose(submission)
      )
      SubmissionResult.Acknowledged
    })

}
