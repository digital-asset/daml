// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, TaskScheduler, TaskSchedulerMetrics}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.SequencerIndexMoved
import com.digitalasset.canton.ledger.participant.state.{SequencerIndex, Update}
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.AcsChange.reassignmentCountersForArchivedTransient
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker
import com.digitalasset.canton.participant.store.ActiveContractSnapshot
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

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
    domainId: DomainId,
    initSc: SequencerCounter,
    initTimestamp: CantonTimestamp,
    ledgerApiIndexer: LedgerApiIndexer,
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    metrics: TaskSchedulerMetrics,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    activeContractSnapshot: ActiveContractSnapshot,
    clock: Clock,
    onSequencerIndexMovingAhead: Traced[SequencerCounterCursorPrehead] => Unit,
)(implicit val executionContextForPublishing: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  private[this] val taskScheduler: TaskScheduler[PublicationTask] =
    TaskScheduler(
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

  private def onlyForTestingRecordAcceptedTransactions(eventO: Option[Traced[Update]]): Unit =
    for {
      store <- ledgerApiIndexer.onlyForTestingTransactionInMemoryStore
      transactionAccepted <- eventO.collect { case Traced(txAccepted: Update.TransactionAccepted) =>
        txAccepted
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
    * @param eventO The update event to be published, or if absent the SequencerCounterMoved event will be published.
    * @param requestCounterO The RequestCounter if applicable.
    *                        For all requests requestCounterO needs to be provided, it is verified internally that
    *                        the RequestCounter space is continuous.
    *                        The provided requestCounterO must match the eventO's DomainIndex if provided.
    */
  def tick(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      eventO: Option[Traced[Update]],
      requestCounterO: Option[RequestCounter],
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (timestamp > initTimestamp) {
      eventO
        .flatMap(_.value.domainIndexOpt)
        .flatMap(_._2.requestIndex)
        .map(_.counter)
        .foreach(requestCounter =>
          logger.debug(s"Schedule publication for request counter $requestCounter")
        )
      onlyForTestingRecordAcceptedTransactions(eventO)
      taskScheduler.scheduleTask(
        EventPublicationTask(
          sequencerCounter,
          timestamp,
        )(
          eventO,
          requestCounterO,
        )
      )
      logger.debug(
        s"Observing time $timestamp for sequencer counter $sequencerCounter for publishing (with eventO:$eventO, requestCounterO:$requestCounterO)"
      )
      // Schedule a time observation task that delays the event publication
      // until the InFlightSubmissionTracker has synchronized with submission registration.
      taskScheduler.scheduleTask(TimeObservationTask(sequencerCounter, timestamp))
      taskScheduler.addTick(sequencerCounter, timestamp)
      // this adds backpressure from indexer queue to protocol processing:
      //   indexer pekko source queue back-pressures via offer Future,
      //   this propagates via in RecoveringQueue,
      //   which propagates here in the taskScheduler's SimpleExecutionQueue,
      //   which bubble up exactly here: waiting for all the possible event enqueueing to happen after the tick.
      taskScheduler.flush()
    } else {
      logger.debug(
        s"Skipping tick at sequencerCounter:$sequencerCounter timestamp:$timestamp (publication of event $eventO)"
      )
      Future.unit
    }

  def scheduleAcsChangePublication(
      recordSequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      requestCounter: RequestCounter,
      commitSet: CommitSet,
  )(implicit traceContext: TraceContext): Unit =
    taskScheduler.scheduleTask(
      AcsChangePublicationTask(recordSequencerCounter, timestamp)(Some((requestCounter, commitSet)))
    )

  /** Schedules an empty acs change publication task to be published to the `acsChangeListener`.
    */
  def scheduleEmptyAcsChangePublication(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Unit =
    if (sequencerCounter >= initSc) {
      taskScheduler.scheduleTask(
        AcsChangePublicationTask(sequencerCounter, timestamp)(None)
      )
    }

  private sealed trait PublicationTask extends TaskScheduler.TimedTask

  private object PublicationTask {
    def orderingSameTimestamp: Ordering[PublicationTask] = Ordering.by(rankSameTimestamp)

    private def rankSameTimestamp(x: PublicationTask): Option[(Option[SequencerCounter], Int)] =
      x match {
        case _: TimeObservationTask =>
          // TimeObservationTask comes first so that we synchronize with the InFlightSubmissionTracker before publishing an event.
          // All TimeObservationTasks with the same timestamp are considered equal.
          None
        case task: EventPublicationTask =>
          Some(
            (
              Some(task.sequencerCounter),
              // EventPublicationTask comes before AcsChangePublicationTask if they have the same sequencer counter.
              // This is necessary to devise the future from the corresponding EventPublicationTask which gate the AcsPublication.
              0,
            )
          )
        case task: AcsChangePublicationTask => Some(Some(task.sequencerCounter) -> 1)
      }
  }

  /** Task to synchronize with the [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]].
    * Must run before the event is published to the event log to make sure that a submission to be registered cannot
    * concurrently be deleted.
    */
  private case class TimeObservationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends PublicationTask {

    override def perform(): FutureUnlessShutdown[Unit] =
      performUnlessClosingUSF("observe-timestamp-task") {
        /*
         * Observe timestamp will only return an UnknownDomain error if the domain ID is not in the connectedDomainMap
         * in the CantonSyncService. When disconnecting from the domain we first remove the domain from the map and only then
         * close the sync domain, so there's a window at the beginning of shutdown where we could get an UnknownDomain error here.
         * To prevent further tasks to be executed we return a 'FutureUnlessShutdown.abortedDueToShutdown' which will
         * cause the execution queue in the task scheduler to abort all subsequent queued tasks instead of running them.
         * Note that in this context 'shutdown' doesn't necessarily mean complete node shutdown, closing of the SyncDomain is enough,
         * which can happen when we disconnect from the domain (without necessarily shutting down the node entirely)
         */
        inFlightSubmissionTracker
          .observeTimestamp(domainId, timestamp)
          .mapK(FutureUnlessShutdown.outcomeK)
          .valueOrF { case InFlightSubmissionTracker.UnknownDomain(_domainId) =>
            logger.info(
              s"Skip the synchronization with the in-flight submission tracker for sequencer counter $sequencerCounter at $timestamp due to shutdown."
            )
            FutureUnlessShutdown.abortedDueToShutdown
          }
      }

    override protected def pretty: Pretty[this.type] = prettyOfClass(
      param("timestamp", _.timestamp),
      param("sequencer counter", _.sequencerCounter),
    )

    override def close(): Unit = ()
  }

  // TODO(i18695): try to remove the Traced[] from the Update here and in protocol processing if possible
  /** Task to publish the event `event` if defined. */
  private[RecordOrderPublisher] case class EventPublicationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
  )(
      val eventO: Option[Traced[Update]],
      val requestCounterO: Option[RequestCounter],
  )(implicit val traceContext: TraceContext)
      extends PublicationTask {

    private val event: Traced[Update] = eventO.getOrElse(
      Traced(
        SequencerIndexMoved(
          domainId = domainId,
          sequencerIndex = SequencerIndex(
            counter = sequencerCounter,
            timestamp = timestamp,
          ),
          requestCounterO = requestCounterO,
        )
      )
    )

    // TODO(i18695): attempt to simplify data structures and data passing, so these assertions can be removed
    assert(event.value.domainIndexOpt.isDefined)
    event.value.domainIndexOpt.foreach { case (_, domainIndex) =>
      assert(domainIndex.sequencerIndex.isDefined)
      domainIndex.sequencerIndex.foreach { sequencerIndex =>
        assert(sequencerIndex.timestamp == timestamp)
        assert(sequencerIndex.counter == sequencerCounter)
      }
      requestCounterO match {
        case Some(requestCounter) =>
          assert(domainIndex.requestIndex.isDefined)
          domainIndex.requestIndex.foreach { requestIndex =>
            assert(requestIndex.counter == requestCounter)
            assert(requestIndex.timestamp == timestamp)
            assert(requestIndex.sequencerCounter == Some(sequencerCounter))
          }

        case None =>
          assert(domainIndex.requestIndex.isEmpty)
      }
    }

    override def perform(): FutureUnlessShutdown[Unit] = performUnlessClosingUSF("publish-event") {
      logger.debug(s"Publish event with domain index ${event.value.domainIndexOpt}")
      FutureUnlessShutdown.outcomeF(
        ledgerApiIndexer.queue
          .offer(event)
          .map { _ =>
            updateLastPublishedPersisted(
              LastPublishedPersisted(
                sequencerCounter = sequencerCounter,
                timestamp = timestamp,
                requestCounterO = requestCounterO,
                updatePersisted = event.value.persisted.future,
              )
            )
            onSequencerIndexMovingAhead(
              Traced(CursorPrehead(sequencerCounter, timestamp))
            )
          }
      )
    }

    override protected def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp),
        param("sequencerCounter", _.sequencerCounter),
        param("event", _.event.value),
      )

    override def close(): Unit = ()
  }

  private case class AcsChangePublicationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
  )(val requestCounterCommitSetPairO: Option[(RequestCounter, CommitSet)])(implicit
      val traceContext: TraceContext
  ) extends PublicationTask {

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

  override def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq(SyncCloseable("taskScheduler", taskScheduler.close()))
}
