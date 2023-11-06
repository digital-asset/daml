// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import cats.Eval
import cats.syntax.foldable.*
import cats.syntax.option.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{
  CantonTimestamp,
  PeanoQueue,
  TaskScheduler,
  TaskSchedulerMetrics,
}
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher.PendingPublish
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  SequencedSubmission,
}
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightBySequencingInfo,
  InFlightReference,
}
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.store.{
  ActiveContractSnapshot,
  EventLogId,
  MultiDomainEventLog,
  SingleDimensionEventLog,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.participant.{LocalOffset, RequestOffset, TopologyOffset}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Publishes upstream events and active contract set changes in the order of their record time.
  *
  * The protocol processors emit events and active contract set changes in the result message order,
  * which differs from the record time (= sequencing time) order.
  * The [[RecordOrderPublisher]] pushes the events to [[store.SingleDimensionEventLog]] and the active contract set changes
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
  * @param eventLog The event log to publish the events to
  * @param metrics The task scheduler metrics
  * @param executionContextForPublishing Execution context for publishing the events
  */
class RecordOrderPublisher(
    domainId: DomainId,
    initSc: SequencerCounter,
    initTimestamp: CantonTimestamp,
    eventLog: SingleDimensionEventLog[DomainEventLogId],
    multiDomainEventLog: Eval[MultiDomainEventLog],
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    metrics: TaskSchedulerMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    activeContractSnapshot: ActiveContractSnapshot,
)(implicit val executionContextForPublishing: ExecutionContext, elc: ErrorLoggingContext)
    extends NamedLogging
    with FlagCloseableAsync {

  // Synchronization to block publication of new events until preceding events recovered by crash recovery
  // have been published
  private val recovered: PromiseUnlessShutdown[Unit] =
    new PromiseUnlessShutdown[Unit]("recovered", futureSupervisor)

  private[this] val taskScheduler: TaskScheduler[PublicationTask] =
    new TaskScheduler(
      initSc,
      initTimestamp,
      PublicationTask.orderingSameTimestamp,
      metrics,
      timeouts,
      loggerFactory.appendUnnamedKey("task scheduler owner", "RecordOrderPublisher"),
      futureSupervisor,
    )

  private val acsChangeListener = new AtomicReference[Option[AcsChangeListener]](None)

  /** Schedules the given `event` to be published on the `eventLog`, and schedules the causal "tick" defined by `clock`.
    *
    * @param requestSequencerCounter The sequencer counter associated with the message that corresponds to the request
    * @param eventO The timestamped event to be published
    */
  def schedulePublication(
      requestSequencerCounter: SequencerCounter,
      requestCounter: RequestCounter,
      requestTimestamp: CantonTimestamp,
      eventO: Option[TimestampedEvent],
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (eventO.isEmpty) Future.unit
    else {
      logger.debug(
        s"Schedule publication for request counter $requestCounter: event = ${eventO.isDefined}"
      )

      for {
        _ <- eventO.traverse_(eventLog.insert(_))
      } yield {
        val inFlightReference =
          InFlightBySequencingInfo(
            domainId,
            SequencedSubmission(requestSequencerCounter, requestTimestamp),
          )
        val task =
          EventPublicationTask(
            requestSequencerCounter,
            RequestOffset(requestTimestamp, requestCounter),
          )(
            eventO,
            Some(inFlightReference),
          )
        taskScheduler.scheduleTask(task)
      }
    }

  /** Schedules the given `event` to be published on the `eventLog`, and schedules the causal "tick" defined by `clock`.
    *
    * @param sequencerCounter The sequencer counter associated with the message that corresponds to the request
    * @param event            The timestamped event to be published
    */
  def schedulePublication(
      sequencerCounter: SequencerCounter,
      topologyOffset: TopologyOffset,
      event: TimestampedEvent,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug(
      s"Schedule publication for offset $topologyOffset derived from sc=$sequencerCounter"
    )

    for {
      _ <- eventLog.insert(event)
    } yield {
      val task =
        EventPublicationTask(sequencerCounter, topologyOffset)(
          Some(event),
          None,
        )
      taskScheduler.scheduleTask(task)
    }
  }

  def scheduleRecoveries(
      toRecover: Seq[PendingPublish]
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Schedule recovery for ${toRecover.length} pending events.")

    val recoverF = MonadUtil.sequentialTraverse_[FutureUnlessShutdown, PendingPublish](toRecover) {
      pendingPublish =>
        logger.info(s"Recover pending causality update $pendingPublish")

        val eventO = pendingPublish match {
          case RecordOrderPublisher.PendingTransferPublish(_ts, _eventLogId) => None
          case RecordOrderPublisher.PendingEventPublish(event, _ts, _eventLogId) => Some(event)
        }

        val inFlightRef = eventO.flatMap(tse =>
          tse.requestSequencerCounter.map(sc =>
            InFlightBySequencingInfo(
              eventLog.id.domainId,
              sequenced = SequencedSubmission(sc, tse.timestamp),
            )
          )
        )

        publishEvent(eventO, inFlightRef)
    }

    recovered.completeWith(recoverF)
    ()
  }

  def scheduleAcsChangePublication(
      recordSequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      requestCounter: RequestCounter,
      commitSet: CommitSet,
  ): Unit = TraceContext.withNewTraceContext { implicit traceContext =>
    taskScheduler.scheduleTask(
      AcsChangePublicationTask(recordSequencerCounter, timestamp)(Some((requestCounter, commitSet)))
    )
  }

  /** Schedules an empty acs change publication task to be published to the `acsChangeListener`.
    */
  def scheduleEmptyAcsChangePublication(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  ): Unit = TraceContext.withNewTraceContext { implicit traceContext =>
    if (sequencerCounter >= initSc) {
      taskScheduler.scheduleTask(
        AcsChangePublicationTask(sequencerCounter, timestamp)(None)
      )
    }
  }

  /** Signals the progression of time to the record order publisher
    * Does not notify the ACS commitment processor.
    *
    * @see TaskScheduler.addTick for the behaviour.
    */
  def tick(sequencerCounter: SequencerCounter, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit =
    if (timestamp > initTimestamp) {
      logger.debug(
        s"Observing time $timestamp for sequencer counter $sequencerCounter for publishing"
      )
      // Schedule a time observation task that delays the event publication
      // until the InFlightSubmissionTracker has synchronized with submission registration.
      taskScheduler.scheduleTask(TimeObservationTask(sequencerCounter, timestamp))
      taskScheduler.addTick(sequencerCounter, timestamp)
    }

  /** The returned future completes after all events that are currently scheduled for publishing have been published. */
  @VisibleForTesting
  def flush(): Future[Unit] = {
    recovered.future.flatMap { _bool =>
      taskScheduler.flush()
    }
  }

  /** Used to inspect the state of the sequencerCounterQueue, for testing purposes. */
  @VisibleForTesting
  def readSequencerCounterQueue(
      sequencerCounter: SequencerCounter
  ): PeanoQueue.AssociatedValue[CantonTimestamp] =
    taskScheduler.readSequencerCounterQueue(sequencerCounter)

  private sealed trait PublicationTask extends TaskScheduler.TimedTask

  private object PublicationTask {
    def orderingSameTimestamp: Ordering[PublicationTask] = Ordering.by(rankSameTimestamp)

    private def rankSameTimestamp(x: PublicationTask): Option[(Option[LocalOffset], Int)] =
      x match {
        case _: TimeObservationTask =>
          // TimeObservationTask comes first so that we synchronize with the InFlightSubmissionTracker before publishing an event.
          // All TimeObservationTasks with the same timestamp are considered equal.
          None
        case task: EventPublicationTask =>
          (
            task.localOffset.some,
            0, // EventPublicationTask comes before AcsChangePublicationTask if they have the same tie breaker. This is an arbitrary decision.
          ).some
        case task: AcsChangePublicationTask => (task.requestOffsetO, 1).some
      }
  }

  /** Task to synchronize with the [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]].
    * Must run before the event is published to the event log to make sure that a submission to be registered cannot
    * concurrently be deleted.
    */
  private case class TimeObservationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext)
      extends PublicationTask {
    override def perform(): FutureUnlessShutdown[Unit] = {
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
    }

    override def pretty: Pretty[this.type] = prettyOfClass(
      param("timestamp", _.timestamp),
      param("sequencer counter", _.sequencerCounter),
    )

    override def close(): Unit = ()
  }

  /** Task to register the causality update `update` and publish the event `event` if defined.
    * Some causality updates are not currently associated with an event, for example transfer-ins
    * require a causality update but are not always associated with an event. This is why the event
    * is optional.
    */
  private[RecordOrderPublisher] case class EventPublicationTask(
      override val sequencerCounter: SequencerCounter,
      localOffset: LocalOffset,
  )(
      val eventO: Option[TimestampedEvent],
      val inFlightReference: Option[InFlightReference],
  )(implicit val traceContext: TraceContext)
      extends PublicationTask {

    def timestamp: CantonTimestamp = localOffset.effectiveTime

    override def perform(): FutureUnlessShutdown[Unit] = {
      for {
        _recovered <- recovered.futureUS
        _unit <- publishEvent(eventO, inFlightReference)
      } yield ()
    }

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp),
        param("sequencerCounter", _.sequencerCounter),
        param("event", _.eventO),
      )

    override def close(): Unit = ()
  }

  private def publishEvent(
      eventO: Option[TimestampedEvent],
      inFlightRef: Option[InFlightReference],
  )(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingUSF("publish-event") {
      for {
        _published <- eventO.fold(FutureUnlessShutdown.unit) { event =>
          logger.debug(s"Publish event with request counter ${event.localOffset}")
          val data = PublicationData(eventLog.id, event, inFlightRef)
          FutureUnlessShutdown.outcomeF(multiDomainEventLog.value.publish(data))
        }
      } yield ()
    }

  // tieBreaker is used to order tasks with the same timestamp
  /*
  Note that we put requestCounter in the second argument list, because it is used together with the commit set.
  This means that the case class's generated equality method compares only timestamp, sequencerCounter.
  However,
  publicationTasks are ordered by
  [[com.digitalasset.canton.participant.event.RecordOrderPublisher.PublicationTask.orderingSameTimestamp]]
  which looks at timestamp, sequencerCounter and requestCounter.
  Thus, the equals method considers more AcsChangePublicationTasks equal than the orderingSameTimestamp.
  However, orderingSameTimestamp is used only for the TaskScheduler, which should not care about equality of tasks.
   */
  private case class AcsChangePublicationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
  )(val requestCounterCommitSetPairO: Option[(RequestCounter, CommitSet)])(implicit
      val traceContext: TraceContext
  ) extends PublicationTask {

    val requestOffsetO: Option[RequestOffset] = requestCounterCommitSetPairO.map { case (rc, _) =>
      RequestOffset(timestamp, rc)
    }

    override def perform(): FutureUnlessShutdown[Unit] = {
      // If the requestCounterCommitSetPairO is not set, then by default the commit set is empty, and
      // the request counter is the smallest possible value that does not throw an exception in
      // ActiveContractStore.bulkContractsTransferCounterSnapshot, i.e., Genesis
      val (requestCounter, commitSet) =
        requestCounterCommitSetPairO.getOrElse((RequestCounter.Genesis, CommitSet.empty))
      // Augments the commit set with the updated transfer counters for archive events,
      // computes the acs change and publishes it
      logger.debug(
        show"The received commit set contains creations ${commitSet.creations}" +
          show"transfer-ins ${commitSet.transferIns}" +
          show"archivals ${commitSet.archivals} transfer-outs ${commitSet.transferOuts}"
      )
      val acsChangePublish =
        for {
          // Retrieves the transfer counters of the archived contracts from the latest state in the active contract store
          archivalsWithTransferCountersOnly <- activeContractSnapshot
            .bulkContractsTransferCounterSnapshot(commitSet.archivals.keySet, requestCounter)

        } yield {
          // Computes the ACS change by decorating the archive events in the commit set with their transfer counters
          val acsChange = AcsChange.fromCommitSet(commitSet, archivalsWithTransferCountersOnly)
          logger.debug(
            s"Computed ACS change activations ${acsChange.activations} deactivations ${acsChange.deactivations}"
          )
          def recordTime: RecordTime =
            RecordTime(
              timestamp,
              requestCounterCommitSetPairO.map(_._1.unwrap).getOrElse(RecordTime.lowestTiebreaker),
            )
          acsChangeListener.get.foreach(_.publish(recordTime, acsChange))
        }
      FutureUnlessShutdown.outcomeF(acsChangePublish)
    }

    override def pretty: Pretty[this.type] =
      prettyOfClass(param("timestamp", _.timestamp), param("sequencerCounter", _.sequencerCounter))

    override def close(): Unit = ()
  }

  def setAcsChangeListener(listener: AcsChangeListener): Unit = {
    acsChangeListener.getAndUpdate {
      case None => Some(listener)
      case Some(_acsChangeListenerAlreadySet) =>
        throw new IllegalArgumentException("ACS change listener already set")
    }.discard
  }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    Seq(
      SyncCloseable("taskScheduler", taskScheduler.close()),
      SyncCloseable("recovered-promise", recovered.shutdown()),
    )
  }
}
object RecordOrderPublisher {
  sealed trait PendingPublish {
    val ts: CantonTimestamp
    val createsEvent: Boolean
    val eventLogId: EventLogId
  }

  final case class PendingTransferPublish(
      ts: CantonTimestamp,
      eventLogId: EventLogId,
  ) extends PendingPublish {
    override val createsEvent: Boolean = false
  }

  final case class PendingEventPublish(
      event: TimestampedEvent,
      ts: CantonTimestamp,
      eventLogId: EventLogId,
  ) extends PendingPublish {
    override val createsEvent: Boolean = true
  }

}
