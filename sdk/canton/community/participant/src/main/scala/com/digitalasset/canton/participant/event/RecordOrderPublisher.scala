// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import cats.implicits.catsSyntaxOptionId
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, TaskScheduler, TaskSchedulerMetrics}
import com.digitalasset.canton.ledger.participant.state.Update.EmptyAcsPublicationRequired
import com.digitalasset.canton.ledger.participant.state.{SequencedUpdate, Update}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

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
  * All sequencer counters above and including `initSc` must eventually be signalled to the [[RecordOrderPublisher]] using [[tick]].
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
    val initTimestamp: CantonTimestamp,
    ledgerApiIndexer: LedgerApiIndexer,
    metrics: TaskSchedulerMetrics,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
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
    * @param event The update event to be published.
    */
  def tick(event: SequencedUpdate)(implicit traceContext: TraceContext): Future[Unit] =
    ifNotClosedYet {
      if (event.recordTime > initTimestamp) {
        event.requestCounterO
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
    * @return The timestamp used for the publication.
    */
  def scheduleFloatingEventPublicationImmediately(
      eventFactory: CantonTimestamp => Option[Update]
  )(implicit traceContext: TraceContext): CantonTimestamp = ifNotClosedYet {
    taskScheduler
      .scheduleTaskImmediately(
        taskFactory = immediateTimestamp =>
          eventFactory(immediateTimestamp) match {
            case Some(event) =>
              logger.debug(
                s"Publish floating event immediately with timestamp $immediateTimestamp"
              )
              ledgerApiIndexer.enqueue(event).map(_ => ())
            case None =>
              logger.debug(
                s"Skip publish-immediately floating event with timestamp $immediateTimestamp: nothing to publish"
              )
              FutureUnlessShutdown.unit
          },
        taskTraceContext = traceContext,
      )
  }

  /** Schedules an empty acs change publication task to be published to the `acsChangeListener`.
    */
  def scheduleEmptyAcsChangePublication(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Unit = ifNotClosedYet {
    if (sequencerCounter >= initSc) {
      scheduleFloatingEventPublication(
        timestamp = timestamp,
        eventFactory = EmptyAcsPublicationRequired(domainId, _).some,
      ).toOption.getOrElse(
        ErrorUtil.invalidState(
          "Trying to schedule empty ACS change publication too late: the specified timestamp is already ticked."
        )
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

    // For each tick we have exactly one EventPublicationTask, which always should precede all the other scheduled
    // floating events.
    private def rankSameTimestamp(x: PublicationTask): (Int, Option[SequencerCounter]) =
      x match {
        case task: EventPublicationTask => 0 -> Some(task.sequencerCounter)
        case _: FloatingEventPublicationTask[_] => 1 -> None
      }
  }

  /** Task to publish the event `event` if defined. */
  private[RecordOrderPublisher] case class EventPublicationTask(event: SequencedUpdate)(implicit
      val traceContext: TraceContext
  ) extends SequencedPublicationTask {

    override val timestamp: CantonTimestamp = event.recordTime
    override val sequencerCounter: SequencerCounter = event.sequencerCounter

    override def perform(): FutureUnlessShutdown[Unit] = {
      logger.debug(s"Publish event with domain index ${event.domainIndex}")
      ledgerApiIndexer
        .enqueue(event)
        .map(_ => ())
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
          eventO() match {
            case Some(event) =>
              logger.debug(s"Publish floating event with timestamp $timestamp")
              ledgerApiIndexer.enqueue(event).map(_ => ())
            case None =>
              logger.debug(
                s"Skip publishing floating event with timestamp $timestamp: nothing to publish"
              )
              FutureUnlessShutdown.unit
          }

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
