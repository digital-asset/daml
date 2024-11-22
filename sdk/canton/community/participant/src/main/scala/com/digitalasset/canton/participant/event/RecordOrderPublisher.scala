// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, TaskScheduler, TaskSchedulerMetrics}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.{SequencedUpdate, Update}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.AcsChange.reassignmentCountersForArchivedTransient
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.store.ActiveContractSnapshot
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Publishes upstream events and active contract set changes in the order of their record time.
  *
  * The protocol processors produce events and active contract set changes in the result message order in phase 7.
  * The [[RecordOrderPublisher]] pushes the events to indexer and the active contract set changes in record time order
  * (which equals sequencing time order).
  * (including empty changes on time proofs) to the appropriate listener, which is normally [[pruning.AcsCommitmentProcessor]].
  * The events are published only after the [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]]
  * has observed the timestamp.
  *
  * All sequencer counters above `initSc` must eventually be signalled to the [[RecordOrderPublisher]] using [[tick]].
  * An event is published only when all sequencer counters between `initSc` and its associated sequencer counter
  * have been signalled.
  *
  * @param initSc The initial sequencer counter from which on events should be published
  * @param initTimestamp The initial timestamp from which on events should be published
  * @param metrics The task scheduler metrics
  * @param executionContextForPublishing Execution context for publishing the events
  */
class RecordOrderPublisher(
    initSc: SequencerCounter,
    val initTimestamp: CantonTimestamp,
    ledgerApiIndexer: LedgerApiIndexer,
    metrics: TaskSchedulerMetrics,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    activeContractSnapshot: ActiveContractSnapshot,
    clock: Clock,
)(implicit val executionContextForPublishing: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync
    with HasCloseContext
    with PromiseUnlessShutdownFactory {

  private[this] val taskScheduler: TaskScheduler[PublicationTask] =
    TaskScheduler[PublicationTask](
      initSc,
      initTimestamp,
      PublicationTask.orderingSameTimestamp,
      metrics,
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      loggerFactory.appendUnnamedKey("task scheduler owner", "RecordOrderPublisher"),
      futureSupervisor,
      clock,
    )

  private val acsChangeListener = new AtomicReference[Option[AcsChangeListener]](None)

  /** This is a heavy handed approach to make the AcsPublication commence only after the corresponding update is persisted.
    * This is needed because the AcsCommitmentProcessor expects that time is not moving backwards, meaning: after recovery
    * we should not have anything reprocessed again, which has been processed already. This can be achieved if we wait for
    * the corresponding Event's Update.persisted future to finish: this means the ledger end is also bumped, therefore any
    * recovery only can start afterwards.
    * This approach is based on the invariant that for each AcsPublicationTask we have a corresponding
    * EventPublicationTask, which is the case since we are emitting for each sequencer counter either an Update event,
    * or the SequencerIndexMoved Update event. Also it is assumed that the EventPublicationTask comes earlier than the
    * AcsPublicationTask.
    * These invariants are verified runtime as it is being used in lastPublishedPersisted below.
    * Possible improvement to this approach: moving AcsPublications to the post-processing stage.
    */
  private case class LastPublishedPersisted(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      requestCounterO: Option[RequestCounter],
      updatePersisted: Future[Unit],
  )

  private val lastPublishedPersistedRef: AtomicReference[Option[LastPublishedPersisted]] =
    new AtomicReference(None)

  private def updateLastPublishedPersisted(lastPublishedPersisted: LastPublishedPersisted): Unit =
    lastPublishedPersistedRef.getAndUpdate { previousO =>
      previousO.foreach { previous =>
        assert(previous.timestamp < lastPublishedPersisted.timestamp)
        assert(previous.sequencerCounter + 1 == lastPublishedPersisted.sequencerCounter)
        lastPublishedPersisted.requestCounterO.foreach(currentRequestCounter =>
          previous.requestCounterO.foreach(previousRequestCounter =>
            assert(previousRequestCounter + 1 == currentRequestCounter)
          )
        )
      }
      Some(lastPublishedPersisted)
    }.discard

  private def lastPublishedPersisted(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  ): Future[Unit] = {
    val currentLastPublishedPersisted = lastPublishedPersistedRef
      .get()
      .getOrElse(
        throw new IllegalStateException(
          "There should always be a last published event by the time anyone waits for it"
        )
      )
    // The two checks below validate the invariant documented above that the EventPublicationTask comes earlier than the AcsPublicationTask.
    // Therefore, it is expected that the event that the ACS commitment processor is waiting to be persisted has indeed been previously published.
    assert(sequencerCounter == currentLastPublishedPersisted.sequencerCounter)
    assert(timestamp == currentLastPublishedPersisted.timestamp)
    currentLastPublishedPersisted.updatePersisted
  }

  private def onlyForTestingRecordAcceptedTransactions(event: SequencedUpdate): Unit =
    for {
      store <- ledgerApiIndexer.onlyForTestingTransactionInMemoryStore
      transactionAccepted <- event match {
        case txAccepted: Update.TransactionAccepted => Some(txAccepted)
        case _ => None
      }
    } {
      store.put(
        updateId = transactionAccepted.updateId,
        lfVersionedTransaction = transactionAccepted.transaction,
      )
    }

  /** Schedules the given `eventO` to be published on the `eventLog`, and schedules the causal "tick" defined by `clock`.
    * Tick must be called exactly once for all sequencer counters higher than initTimestamp.
    *
    * @param event The update event to be published, or if absent the SequencerCounterMoved event will be published.
    */
  def tick(event: SequencedUpdate)(implicit traceContext: TraceContext): Future[Unit] =
    ifNotClosedYet {
      if (event.recordTime > initTimestamp) {
        event.domainIndexOpt
          .flatMap(_._2.requestIndex)
          .map(_.counter)
          .foreach(requestCounter =>
            logger.debug(s"Schedule publication for request counter $requestCounter")
          )
        onlyForTestingRecordAcceptedTransactions(event)
        taskScheduler.scheduleTask(EventPublicationTask(event))
        logger.debug(
          s"Observing time ${event.recordTime} for sequencer counter ${event.sequencerCounter} for publishing (with event:$event, requestCounterO:${event.requestCounterO})"
        )
        taskScheduler.addTick(event.sequencerCounter, event.recordTime)
        // this adds backpressure from indexer queue to protocol processing:
        //   indexer pekko source queue back-pressures via offer Future,
        //   this propagates via in RecoveringQueue,
        //   which propagates here in the taskScheduler's SimpleExecutionQueue,
        //   which bubble up exactly here: waiting for all the possible event enqueueing to happen after the tick.
        taskScheduler.flush()
      } else {
        logger.debug(
          s"Skipping tick at sequencerCounter:${event.sequencerCounter} timestamp:${event.recordTime} (publication of event $event)"
        )
        Future.unit
      }
    }

  /** Schedule a floating event, if the current domain time is earlier than timestamp.
    * @param timestamp The desired timestamp of the publication: if cannot be met, the function will return a Left.
    * @param eventFactory A function returning an optional Update to be published.
    *                     This function will be executed as the scheduled task is executing. (if scheduling is possible)
    *                     This function will be called with timestamp.
    * @param onScheduled A function creating a FutureUnlessShutdown[T].
    *                    This function will be only executed, if the scheduling is possible.
    *                    This function will be executed before the scheduleFloatingEventPublication returns. (if scheduling is possible)
    *                    Execution of the floating event publication will wait for the onScheduled operation to finish.
    * @param traceContext Should be the TraceContext of the event
    * @return A Left with the current domain time, if scheduling is not possible as the domain time is bigger.
    *         A Right with the result of the onScheduled FutureUnlessShutdown[T].
    */
  def scheduleFloatingEventPublication[T](
      timestamp: CantonTimestamp,
      eventFactory: CantonTimestamp => Option[Update],
      onScheduled: () => FutureUnlessShutdown[T], // perform will wait for this to complete
  )(implicit traceContext: TraceContext): Either[CantonTimestamp, FutureUnlessShutdown[T]] =
    ifNotClosedYet {
      taskScheduler
        .scheduleTaskIfLater(
          desiredTimestamp = timestamp,
          taskFactory = _ => {
            val resultFUS = onScheduled()
            FloatingEventPublicationTask(
              waitFor = resultFUS,
              timestamp = timestamp,
            )(() => eventFactory(timestamp))
          },
        )
        .map(_.waitFor)
    }

  /** Schedule a floating event, if the current domain time is earlier than timestamp.
    * @param timestamp The desired timestamp of the publication: if cannot be met, the function will return a Left.
    * @param eventFactory A function returning an optional Update to be published.
    *                     This function will be executed as the scheduled task is executing. (if scheduling is possible)
    *                     This function will be called with timestamp.
    * @param traceContext Should be the TraceContext of the event
    * @return A Left with the current domain time, if scheduling is not possible as the domain time is bigger.
    *         A Right with unit if possible.
    */
  def scheduleFloatingEventPublication(
      timestamp: CantonTimestamp,
      eventFactory: CantonTimestamp => Option[Update],
  )(implicit traceContext: TraceContext): Either[CantonTimestamp, Unit] =
    scheduleFloatingEventPublication(
      timestamp = timestamp,
      eventFactory = eventFactory,
      onScheduled = () => FutureUnlessShutdown.unit,
    ).map(_ => ())

  /** Schedule a floating event immediately: with the domain time of the last published event.
    * @param eventFactory A function returning an optional Update to be published.
    *                     This function will be executed as the scheduled task is executing.
    *                     This function will be called with the realized domain timestamp.
    * @param traceContext Should be the TraceContext of the event
    * @return
    */
  def scheduleFloatingEventPublicationImmediately(
      eventFactory: CantonTimestamp => Option[Update]
  )(implicit traceContext: TraceContext): CantonTimestamp = ifNotClosedYet {
    taskScheduler
      .scheduleTaskImmediately(
        taskFactory = immediateTimestamp =>
          FutureUnlessShutdown.outcomeF(
            eventFactory(immediateTimestamp) match {
              case Some(event) =>
                logger.debug(
                  s"Publish floating event immediately with timestamp $immediateTimestamp"
                )
                ledgerApiIndexer.queue.offer(event).map(_ => ())
              case None =>
                logger.debug(
                  s"Skip publish-immediately floating event with timestamp $immediateTimestamp: nothing to publish"
                )
                Future.unit
            }
          ),
        taskTraceContext = traceContext,
      )
  }

  def scheduleAcsChangePublication(
      recordSequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      requestCounter: RequestCounter,
      commitSet: CommitSet,
  )(implicit traceContext: TraceContext): Unit = ifNotClosedYet {
    taskScheduler.scheduleTask(
      AcsChangePublicationTask(recordSequencerCounter, timestamp)(Some((requestCounter, commitSet)))
    )
  }

  /** Schedules an empty acs change publication task to be published to the `acsChangeListener`.
    */
  def scheduleEmptyAcsChangePublication(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Unit = ifNotClosedYet {
    if (sequencerCounter >= initSc) {
      taskScheduler.scheduleTask(
        AcsChangePublicationTask(sequencerCounter, timestamp)(None)
      )
    }
  }

  private def ifNotClosedYet[T](t: => T)(implicit traceContext: TraceContext): T =
    if (isClosing) ErrorUtil.invalidState("RecordOrderPublisher should not be used after closed")
    else t

  private sealed trait PublicationTask extends TaskScheduler.TimedTask

  private sealed trait SequencedPublicationTask
      extends PublicationTask
      with TaskScheduler.TimedTaskWithSequencerCounter

  private object PublicationTask {
    def orderingSameTimestamp: Ordering[PublicationTask] = Ordering.by(rankSameTimestamp)

    private def rankSameTimestamp(x: PublicationTask): (Option[SequencerCounter], Int) =
      x match {
        case _: FloatingEventPublicationTask[_] => None -> 0
        case task: EventPublicationTask =>
          (
            Some(task.sequencerCounter),
            // EventPublicationTask comes before AcsChangePublicationTask if they have the same sequencer counter.
            // This is necessary to devise the future from the corresponding EventPublicationTask which gate the AcsPublication.
            0,
          )
        case task: AcsChangePublicationTask => Some(task.sequencerCounter) -> 1
      }
  }

  /** Task to publish the event `event` if defined. */
  private[RecordOrderPublisher] case class EventPublicationTask(event: SequencedUpdate)(implicit
      val traceContext: TraceContext
  ) extends SequencedPublicationTask {

    override val timestamp: CantonTimestamp = event.recordTime
    override val sequencerCounter: SequencerCounter = event.sequencerCounter

    override def perform(): FutureUnlessShutdown[Unit] = {
      logger.debug(s"Publish event with domain index ${event.domainIndexOpt}")
      FutureUnlessShutdown.outcomeF(
        ledgerApiIndexer.queue
          .offer(event)
          .map { _ =>
            updateLastPublishedPersisted(
              LastPublishedPersisted(
                sequencerCounter = sequencerCounter,
                timestamp = timestamp,
                requestCounterO = event.requestCounterO,
                updatePersisted = event.persisted.future,
              )
            )
          }
      )
    }

    override protected def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp),
        param("sequencerCounter", _.sequencerCounter),
        param("event", _.event),
      )

    override def close(): Unit = ()
  }

  /** Task to publish floating event. */
  private[RecordOrderPublisher] case class FloatingEventPublicationTask[T](
      waitFor: FutureUnlessShutdown[T], // ability to hold back publication execution
      override val timestamp: CantonTimestamp,
  )(
      eventO: () => Option[Update]
  )(implicit val traceContext: TraceContext)
      extends PublicationTask {

    override def perform(): FutureUnlessShutdown[Unit] =
      waitFor.transformWith {
        case Success(Outcome(_)) =>
          FutureUnlessShutdown.outcomeF(
            eventO() match {
              case Some(event) =>
                logger.debug(s"Publish floating event with timestamp $timestamp")
                ledgerApiIndexer.queue.offer(event).map(_ => ())
              case None =>
                logger.debug(
                  s"Skip publishing floating event with timestamp $timestamp: nothing to publish"
                )
                Future.unit
            }
          )

        case Success(AbortedDueToShutdown) =>
          logger.debug(
            s"Skip publishing floating event with timestamp $timestamp due to shutting down"
          )
          FutureUnlessShutdown.unit

        case Failure(err) =>
          logger.debug(
            s"Skip publishing floating event with timestamp $timestamp due to failed pre-requisite operation",
            err,
          )
          FutureUnlessShutdown.unit
      }

    override protected def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp)
      )

    override def close(): Unit = ()
  }

  private case class AcsChangePublicationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
  )(val requestCounterCommitSetPairO: Option[(RequestCounter, CommitSet)])(implicit
      val traceContext: TraceContext
  ) extends SequencedPublicationTask {

    override def perform(): FutureUnlessShutdown[Unit] = {
      // If the requestCounterCommitSetPairO is not set, then by default the commit set is empty, and
      // the request counter is the smallest possible value that does not throw an exception in
      // ActiveContractStore.bulkContractsReassignmentCounterSnapshot, i.e., Genesis
      val (requestCounter, commitSet) =
        requestCounterCommitSetPairO.getOrElse((RequestCounter.Genesis, CommitSet.empty))
      // Augments the commit set with the updated reassignment counters for archive events,
      // computes the acs change and publishes it
      logger.trace(
        show"The received commit set contains creations ${commitSet.creations}" +
          show"assignments ${commitSet.assignments}" +
          show"archivals ${commitSet.archivals} unassignments ${commitSet.unassignments}"
      )

      val transientArchivals = reassignmentCountersForArchivedTransient(commitSet)

      val acsChangePublish =
        for {
          // Retrieves the reassignment counters of the archived contracts from the latest state in the active contract store
          archivalsWithReassignmentCountersOnly <- activeContractSnapshot
            .bulkContractsReassignmentCounterSnapshot(
              commitSet.archivals.keySet -- transientArchivals.keySet,
              requestCounter,
            )

        } yield {
          // Computes the ACS change by decorating the archive events in the commit set with their reassignment counters
          val acsChange = AcsChange.tryFromCommitSet(
            commitSet,
            archivalsWithReassignmentCountersOnly,
            transientArchivals,
          )
          logger.debug(
            s"Computed ACS change activations ${acsChange.activations.size} deactivations ${acsChange.deactivations.size}"
          )
          // we only log the full list of changes on trace level
          logger.trace(
            s"Computed ACS change activations ${acsChange.activations} deactivations ${acsChange.deactivations}"
          )
          def recordTime: RecordTime =
            RecordTime(
              timestamp,
              requestCounterCommitSetPairO.map(_._1.unwrap).getOrElse(RecordTime.lowestTiebreaker),
            )
          val waitForLastEventPersisted = lastPublishedPersisted(sequencerCounter, timestamp)
          // The trace context is deliberately generated here instead of continuing the previous one
          // to unlink the asynchronous acs commitment processing from message processing trace.
          TraceContext.withNewTraceContext { implicit traceContext =>
            acsChangeListener.get.foreach(
              _.publish(recordTime, acsChange, waitForLastEventPersisted)
            )
          }
        }
      FutureUnlessShutdown.outcomeF(acsChangePublish)
    }

    override protected def pretty: Pretty[this.type] =
      prettyOfClass(param("timestamp", _.timestamp), param("sequencerCounter", _.sequencerCounter))

    override def close(): Unit = ()
  }

  def setAcsChangeListener(listener: AcsChangeListener): Unit =
    acsChangeListener.getAndUpdate {
      case None => Some(listener)
      case Some(_acsChangeListenerAlreadySet) =>
        throw new IllegalArgumentException("ACS change listener already set")
    }.discard

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    Seq(
      AsyncCloseable(
        "taskScheduler-flush",
        taskScheduler.flush(),
        timeouts.shutdownProcessing,
      ),
      SyncCloseable("taskScheduler", taskScheduler.close()),
    )
  }
}
