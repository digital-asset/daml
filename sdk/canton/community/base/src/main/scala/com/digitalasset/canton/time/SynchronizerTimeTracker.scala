// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.Foldable
import cats.syntax.foldable.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.{ProcessingTimeout, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.{Envelope, TimeProof}
import com.digitalasset.canton.sequencing.{
  BoxedEnvelope,
  OrdinaryApplicationHandler,
  OrdinaryEnvelopeBox,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.time.SynchronizerTimeTracker.*
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

/** Provides a variety of methods for tracking time on the synchronizer.
  *   - fetchTime and fetchTimeProof allows for proactively asking for a recent time or time proof.
  *   - requestTick asks the tracker to ensure that an event is witnessed for the given time or
  *     greater (useful for timeouts).
  *   - awaitTick will return a future to wait for the given time being reached on the target
  *     synchronizer.
  *
  * We currently assume that the synchronizer and our host are roughly synchronized and typically
  * won't expect to see a time on a synchronizer until we have passed that point on our local clock.
  * We then wait for `observationLatency` past the timestamp we are expecting to elapse on our local
  * clock as transmission of an event with that timestamp will still take some time to arrive at our
  * host. This avoids frequently asking for times before we've reached the timestamps we're looking
  * for locally.
  *
  * We also take into account a `patienceDuration` that will cause us to defer asking for a time if
  * we have recently seen events for the synchronizer. This is particularly useful if we are
  * significantly behind and reading many old events from the synchronizer.
  *
  * If no activity is happening on the synchronizer we will try to ensure that we have observed an
  * event at least once during the `minObservationDuration`.
  */
class SynchronizerTimeTracker(
    val config: SynchronizerTimeTrackerConfig,
    clock: Clock,
    timeRequestSubmitter: TimeProofRequestSubmitter,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasFlushFuture {

  /** Timestamps that we are waiting to observe held in ascending order. Queue access must be made
    * while holding the [[lock]].
    *
    * Note: Timestamps in [[pendingTicks]] may be below [[latestTime]] even though [[requestTicks]]
    * and [[awaitTick]] check for this prior to insertion. The issue is that [[latestTime]] may be
    * bumped while the insertion into [[pendingTicks]] is running. This can lead to unnecessary time
    * proof requests. We currently accept this limitation in favor of code simplicity.
    */
  private val pendingTicks: PriorityBlockingQueue[PendingTick] =
    new PriorityBlockingQueue[PendingTick](
      PriorityBlockingQueueUtil.DefaultInitialCapacity,
      Ordering[PendingTick],
    )

  /** Ensures that changes to [[timestampRef]] and [[pendingTicks]] happen atomically */
  private val lock: AnyRef = new Object

  private def withLock[A](fn: => A): A =
    blocking {
      lock.synchronized(fn)
    }

  // the maximum timestamp we can support waiting for without causing an overflow
  private val maxPendingTick = CantonTimestamp.MaxValue.minus(config.observationLatency.asJava)

  private val timestampRef: AtomicReference[LatestAndNext[CantonTimestamp]] =
    new AtomicReference[LatestAndNext[CantonTimestamp]](LatestAndNext.empty)

  private val timeProofRef: AtomicReference[LatestAndNext[TimeProof]] =
    new AtomicReference[LatestAndNext[TimeProof]](LatestAndNext.empty)

  // kick off the scheduling to ensure we see timestamps at least occasionally
  ensureMinObservationDuration()

  /** Fetch the latest timestamp we have observed from the synchronizer. Note this isn't restored on
    * startup so will be empty until the first event after starting is seen.
    */
  def latestTime: Option[CantonTimestamp] = timestampRef.get().latest.map(_.value)

  /** Fetches a recent synchronizer timestamp. If the latest received event has been received within
    * the given `freshnessBound` (measured on the participant clock) this synchronizer timestamp
    * will be immediately returned. If a sufficiently fresh timestamp is unavailable then a request
    * for a time proof will be made, however the returned future will be resolved by the first event
    * after this call (which may not necessarily be the response to our time proof request).
    *
    * @return
    *   The future completes with the synchronizer's timestamp of the event. So if the participant's
    *   local clock is ahead of the synchronizer clock, the timestamp may be earlier than now minus
    *   the freshness bound.
    */
  def fetchTime(freshnessBound: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp] =
    fetch(freshnessBound, timestampRef, requiresTimeProof = false)

  /** Similar to `fetchTime` but will only return time proof. */
  def fetchTimeProof(freshnessBound: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[TimeProof] =
    fetch(freshnessBound, timeProofRef, requiresTimeProof = true)

  /** Register that we want to observe a synchronizer time. The tracker will attempt to make sure
    * that we observe a sequenced event with this timestamp or greater. For ticks below *
    * [[latestTime]], observation has already happened and no further action is needed. If
    * "immediately" is configured and the clock is a [[com.digitalasset.canton.time.SimClock]], a
    * new time proof will be fetched.
    *
    * The maximum timestamp that we support waiting for is [[data.CantonTimestamp.MaxValue]] minus
    * the configured observation latency. If a greater value is provided a warning will be logged
    * but no error will be thrown or returned.
    */
  def requestTick(ts: CantonTimestamp, immediately: Boolean = false)(implicit
      traceContext: TraceContext
  ): TickRequest =
    checked(requestTicks(Seq(Traced(ts)), immediately).apply(0))

  /** Register that we want to observe synchronizer times. The tracker will attempt to make sure
    * that we observe a sequenced event with the given timestamps or greater. For ticks below
    * [[latestTime]], observation has already happened and no further action is needed. If
    * "immediately" is configured and the clock is a [[com.digitalasset.canton.time.SimClock]], a
    * new time proof will be fetched.
    *
    * The maximum timestamp that we support waiting for is [[data.CantonTimestamp.MaxValue]] minus
    * the configured observation latency. If a greater value is provided a warning will be logged
    * but no error will be thrown or returned.
    */
  def requestTicks(timestamps: Seq[Traced[CantonTimestamp]], immediately: Boolean = false)(implicit
      traceContext: TraceContext
  ): Seq[TickRequest] = {
    val tooLarge = timestamps.filter(_.value >= maxPendingTick)

    NonEmpty.from(tooLarge).foreach { tooLarge =>
      val first = tooLarge.min1.value
      val last = tooLarge.max1.value
      logger.warn(
        s"Ignoring request for ${tooLarge.map(_.value).size} ticks from $first to $last as they are too large"
      )
    }

    val theLatestTime = latestTime
    // Ignore all requests that are not above the latest time we have observed and those that are too large.
    val (ticksAboveBound, tickRequests) =
      timestamps.foldLeft(Vector.empty[RequestingTick] -> Vector.empty[TickRequest]) {
        case ((ticksAboveBound, tickRequests), tracedTick) =>
          implicit val traceContext: TraceContext = tracedTick.traceContext
          val tick = tracedTick.value
          if (theLatestTime.forall(_ < tick) && tick < maxPendingTick) {
            val requestingTick = new RequestingTick(tick)
            (ticksAboveBound :+ requestingTick, tickRequests :+ requestingTick)
          } else (ticksAboveBound, tickRequests :+ DummyTickRequest)
      }
    if (ticksAboveBound.nonEmpty) {
      withLock {
        ticksAboveBound.foreach(pendingTicks.put)
      }
      maybeScheduleUpdate(immediately)
    }
    tickRequests
  }

  /** Waits for an event with a timestamp greater or equal to `ts` to be observed from the
    * synchronizer. If we have already witnessed an event with a timestamp equal or exceeding the
    * given `ts` then `None` will be returned.
    */
  def awaitTick(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): Option[Future[Unit]] = {
    val latest = timestampRef.get().latest
    if (latest.exists(_.value >= ts)) {
      logger.debug(s"No await time for $ts as we are already at $latest")
      None
    } else {
      logger.debug(s"Await time for $ts as we are at ${latest.map(_.value)} ")
      // wait for this timestamp to be observed
      val awaitingTick = new AwaitingTick(ts)
      withLock {
        pendingTicks.put(awaitingTick)
      }
      maybeScheduleUpdate()
      awaitingTick.promise.future.some
    }
  }

  def flow[F[_], Env <: Envelope[_]](implicit F: Foldable[F]): Flow[
    F[BoxedEnvelope[OrdinaryEnvelopeBox, Env]],
    F[BoxedEnvelope[OrdinaryEnvelopeBox, Env]],
    NotUsed,
  ] = Flow[F[BoxedEnvelope[OrdinaryEnvelopeBox, Env]]].map { tracedEventsF =>
    tracedEventsF.toIterable.foreach(_.withTraceContext { implicit batchTraceContext => events =>
      update(events)
    })
    tracedEventsF
  }

  /** Create a [[sequencing.OrdinaryApplicationHandler]] for updating this time tracker */
  def wrapHandler[Env <: Envelope[_]](
      handler: OrdinaryApplicationHandler[Env]
  ): OrdinaryApplicationHandler[Env] = handler.replace { tracedEvents =>
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      update(events)

      // call the wrapped handler
      handler(tracedEvents)
    }
  }

  /** Inform the synchronizer time tracker about the first message the sequencer client resubscribes
    * to from the sequencer. This is never considered a time proof event.
    */
  def subscriptionResumesAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Unit =
    withLock {
      logger.debug(s"Initializing synchronizer time tracker for resubscription at $timestamp")
      updateTimestampRef(timestamp)
      removeTicks(timestamp)
    }

  @VisibleForTesting
  private[time] def update(events: Seq[OrdinarySequencedEvent[Envelope[_]]])(implicit
      batchTraceContext: TraceContext
  ): Unit = {
    withLock {
      def updateOne(event: OrdinarySequencedEvent[Envelope[_]]): Unit = {
        updateTimestampRef(event.timestamp)
        TimeProof.fromEventO(event).foreach { proof =>
          val oldTimeProof = timeProofRef.getAndSet(LatestAndNext(received(proof).some, None))
          oldTimeProof.next.foreach(_.trySuccess(UnlessShutdown.Outcome(proof)))
          timeRequestSubmitter.handleTimeProof(proof)
        }
      }

      // currently all actions from events are synchronous and do not return errors so this simple processing is safe.
      // for timestamps we could just take the latest event in batch, however as we're also looking for time proofs
      // we supply every event sequentially.
      // this could likely be optimised to just process the latest time proof and timestamp from the batch if required.
      events.foreach(updateOne)
      events.lastOption.foreach(event => removeTicks(event.timestamp))
    }
    maybeScheduleUpdate()
  }

  /** Must only be used inside [[withLock]] */
  private def updateTimestampRef(newTimestamp: CantonTimestamp): Unit = {
    val oldTimestamp =
      timestampRef.getAndSet(LatestAndNext(received(newTimestamp).some, None))
    oldTimestamp.next.foreach(_.trySuccess(UnlessShutdown.Outcome(newTimestamp)))
  }

  /** Must only be used inside [[withLock]] */
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def removeTicks(ts: CantonTimestamp): Unit =
    // remove pending ticks up to and including this timestamp
    while (Option(pendingTicks.peek()).exists(_.desiredTimestamp <= ts)) {
      val removed = pendingTicks.poll()
      // complete any futures waiting for them
      removed.complete()
    }

  private def fetch[A](
      freshnessBound: NonNegativeFiniteDuration,
      latestAndNextRef: AtomicReference[LatestAndNext[A]],
      requiresTimeProof: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[A] =
    synchronizeWithClosingSync(functionFullName) {
      val now = clock.now
      val receivedWithin = now.minus(freshnessBound.unwrap)

      val (future, needUpdate) = withLock {
        val newState = latestAndNextRef.updateAndGet { latestAndNext =>
          latestAndNext.latest match {
            case Some(Received(_value, receivedAt)) if receivedAt >= receivedWithin =>
              latestAndNext
            case _latest => latestAndNext.withNextSet
          }
        }
        newState.latest match {
          case Some(Received(value, receivedAt)) if receivedAt >= receivedWithin =>
            FutureUnlessShutdown.pure(value) -> false
          case _ =>
            val promise = newState.next.getOrElse(
              ErrorUtil.internalError(
                new IllegalStateException("Should have set to a promise in prior block")
              )
            )

            // if we're looking for a time proof then just request one; no need to call `maybeScheduleUpdate()`
            // as the TimeProofRequestSubmitter itself retries if it doesn't get one soon enough.
            // otherwise if looking for a timestamp we don't care what synchronizer time we're looking for (just the next),
            // so just register a pending tick for the earliest point.
            // we use MinValue rather than Epoch so it will still be considered far before "now" when initially started
            // using the simclock.
            if (requiresTimeProof) timeRequestSubmitter.fetchTimeProof()
            else pendingTicks.put(new AwaitingTick(CantonTimestamp.MinValue))
            FutureUnlessShutdown(promise.future) -> !requiresTimeProof
        }
      }
      if (needUpdate) maybeScheduleUpdate()
      future
    }.onShutdown(FutureUnlessShutdown.abortedDueToShutdown)

  /** When we expect to observe the earliest timestamp in local time. */
  @VisibleForTesting
  private[time] def earliestExpectedObservationTime(): Option[(Traced[CantonTimestamp], String)] =
    firstUncancelledTick().map(tick =>
      Traced(tick.desiredTimestamp.add(config.observationLatency.asJava))(
        tick.traceContext
      ) -> tick.creationStackTrace
    )

  private[this] def firstUncancelledTick(): Option[PendingTick] =
    withLock {
      @tailrec def go(): Option[PendingTick] =
        Option(pendingTicks.peek()) match {
          case firstO @ Some(first) =>
            if (first.hasBeenCancelled) {
              pendingTicks.poll().discard
              go()
            } else firstO
          case None => None
        }
      go()
    }

  /** Local time of when we'd like to see the next event produced. If we are waiting to observe a
    * timestamp, this value will be the greater (see note below) of:
    *   - the local time of when we'd like to see the earliest tick
    *   - the time we last received an event offset plus the configured patience duration
    *
    * This allows doing nothing for a long period if the timestamp we're looking at is far in the
    * future. However if the synchronizer is far behind but regularly producing events we will wait
    * until we haven't witnessed an event for the patience duration.
    *
    * Note: for sim clock, we always take the earliestExpectedObservationTime. The reason is that
    * progressing the clock may lead to sudden big differences between local clock and timestamps on
    * sequencer messages which lead to some check that decides whether a time proof should be
    * requested not being done.
    *
    * The issue arise in the following case:
    *   - Check is scheduled at t1
    *   - Time is progressed at t3 > t1
    *   - An event is received with sequencing time t2, with t1 < t2 < t3
    *   - Then, the max would lead to t3 which skips the request for a time proof
    */
  private def nextScheduledCheck(): Option[(Traced[CantonTimestamp], String)] =
    // if we're not waiting for an event, then we don't need to see one
    // Only request an event if the time tracker has observed a time;
    // otherwise the submission may fail because the node does not have any signing keys registered
    earliestExpectedObservationTime().flatMap {
      case (earliestExpectedObservationTime, initiatorStackTrace) =>
        val latest = timestampRef.get().latest
        if (latest.isEmpty) {
          logger.debug(
            s"Not scheduling a next check at ${earliestExpectedObservationTime.value} because no timestamp has been observed from the synchronizer"
          )(earliestExpectedObservationTime.traceContext)
        }

        val timeFromReceivedEvent = latest.map(_.receivedAt.add(config.patienceDuration.asJava))

        clock match {
          case _: SimClock =>
            latest.map(_ => earliestExpectedObservationTime -> initiatorStackTrace)
          case _ =>
            timeFromReceivedEvent.map(received =>
              earliestExpectedObservationTime.map(_.max(received))
                -> initiatorStackTrace
            )
        }
    }

  /** we're unable to cancel an update once scheduled, so if we decide to schedule an earlier update
    * than an update already scheduled we update this to the earlier value and then check this value
    * when the scheduled task is run
    */
  private val nextScheduledUpdate: AtomicReference[Option[CantonTimestamp]] =
    new AtomicReference[Option[CantonTimestamp]](None)

  /** After [[pendingTicks]] or [[timestampRef]] have been updated, call this to determine whether a
    * scheduled update is required. It will be scheduled if there isn't an existing or earlier
    * update pending and the time tracker has observed at least some timestamp or if "immediately"
    * is true and the clock is a SimClock.
    */
  private def maybeScheduleUpdate(
      immediately: Boolean = false
  )(implicit traceContext: TraceContext): Unit = {

    def updateNow(tc: TraceContext): Unit =
      // Fine to repeatedly call without guards as the submitter will make no more than one request in-flight at once
      // The next call to update will complete the promise in `timestampRef.get().next`.
      timeRequestSubmitter.fetchTimeProof()(tc)

    if (clock.isSimClock && immediately) {
      if (PrintCallStackForExecutedTimeProofRequests) {
        val callStack = callStackForExecutedTimeProofRequest()
        logger.debug(s"Call stack for immediate time proof request:\n  $callStack")
      }
      updateNow(traceContext)
    } else {
      nextScheduledCheck().foreach { case (updateBy, initiatorStackTrace) =>
        // if we've already surpassed when we wanted to see a time, just ask for one
        // means that we're waiting on a timestamp and we're not receiving regular updates
        val now = clock.now
        if (updateBy.value <= now) {
          if (PrintCallStackForExecutedTimeProofRequests)
            logger.debug(s"Call stack for scheduled time proof request:\n  $initiatorStackTrace")
          updateNow(updateBy.traceContext)
        } else {
          def updateCondition(current: Option[CantonTimestamp]): Boolean = current match {
            case Some(ts) => ts <= now || updateBy.value < ts
            case None => true
          }

          val current = nextScheduledUpdate.getAndUpdate { current =>
            if (updateCondition(current)) updateBy.value.some else current
          }
          if (updateCondition(current)) {
            // schedule next update
            val nextF =
              clock
                .scheduleAt(
                  _ => maybeScheduleUpdate(immediately = false),
                  updateBy.value,
                )
                .unwrap
            addToFlushAndLogError(s"scheduled update at $updateBy")(nextF)
          }
        }
      }
    }
  }

  private def received[A](value: A) = Received(value, receivedAt = clock.now)

  @VisibleForTesting
  protected[time] def flush(): Future[Unit] = doFlush()

  override def onClosed(): Unit = {
    Seq(timeProofRef, timestampRef).foreach { ref =>
      ref.get().next.foreach(_.trySuccess(UnlessShutdown.AbortedDueToShutdown))
    }
    LifeCycle.close(timeRequestSubmitter)(logger)
  }

  /** In the absence of any real activity on the synchronizer we will infrequently request a time.
    * Short of being aware of a relatively recent synchronizer time, it will allow features like
    * sequencer pruning to keep a relatively recent acknowledgment point for the member even if
    * they're not doing anything.
    */
  private def ensureMinObservationDuration(): Unit = withNewTraceContext("min_observation") {
    implicit traceContext =>
      val minObservationDuration = config.minObservationDuration.asJava
      def performUpdate(expectedUpdateBy: CantonTimestamp): Unit =
        synchronizeWithClosingSync(functionFullName) {
          val lastObserved = timestampRef.get().latest.map(_.receivedAt)

          // did we see an event within the observation window
          if (lastObserved.exists(_ >= expectedUpdateBy.minus(minObservationDuration))) {
            // we did
            scheduleNextUpdate()
          } else {
            // we didn't so ask for a time
            logger.debug(
              s"The minimum observation duration $minObservationDuration has elapsed since last observing the synchronizer time (${lastObserved.map(_.toString).getOrElse("never")}) so will request a proof of time"
            )
            FutureUtil.doNotAwait(
              // fetchTime shouldn't fail (if anything it will never complete due to infinite retries or closing)
              // but ensure schedule is called regardless
              fetchTime()
                .thereafter(_ => scheduleNextUpdate())
                .onShutdown(logger.debug("Stopped fetch time due to shutdown")),
              "Failed to fetch a time to ensure the minimum observation duration",
            )
          }
        }.onShutdown(())

      def scheduleNextUpdate(): Unit =
        synchronizeWithClosingSync(functionFullName) {
          val latestTimestamp = timestampRef.get().latest.fold(clock.now)(_.receivedAt)
          val expectUpdateBy = latestTimestamp.add(minObservationDuration).immediateSuccessor

          val _ = clock.scheduleAt(performUpdate, expectUpdateBy)
        }.onShutdown(())

      scheduleNextUpdate()
  }

}

object SynchronizerTimeTracker {

  /** Handle returned by [[SynchronizerTimeTracker.requestTick]] that allows the caller to cancel
    * the request.
    */
  sealed trait TickRequest {
    def cancel(): Unit
  }

  case object DummyTickRequest extends SynchronizerTimeTracker.TickRequest {
    override def cancel(): Unit = ()
  }

  /** A cell for storing [[TickRequest]]s. */
  trait TickRequestCell {

    /** Sets the cell content to the given [[TickRequest]]. If the cell already contains a tick
      * request, then this one will be cancelled. Also cancelled the given [[TickRequest]] if the
      * cell thinks that the tick is no longer needed.
      */
    def setRequest(tickRequest: TickRequest): Unit
  }

  class TickRequestTracker extends TickRequestCell {
    import TickRequestTracker.*
    private val tickRequestState: AtomicReference[TickRequestState] =
      new AtomicReference[TickRequestState](TickRequestState.NotRequested)

    override def setRequest(tickRequest: TickRequest): Unit = {
      val previousState = tickRequestState.getAndUpdate {
        case TickRequestState.NotNeededAnyMore =>
          TickRequestState.NotNeededAnyMore
        case _ => TickRequestState.Requested(tickRequest)
      }
      previousState match {
        case TickRequestState.NotRequested =>
        case TickRequestState.NotNeededAnyMore =>
          // Immediately cancel the requested tick as it's not needed any more
          tickRequest.cancel()
        case TickRequestState.Requested(prior) => prior.cancel()
      }
    }

    /** Cancels the current (if any) and all subsequent [[TickRequest]]s given to [[setRequest]] */
    def cancel(): Unit = {
      val previousState = tickRequestState.getAndSet(TickRequestState.NotNeededAnyMore)
      previousState match {
        case TickRequestState.Requested(requestedTick) => requestedTick.cancel()
        case TickRequestState.NotRequested |
            TickRequestState.NotNeededAnyMore => // nothing to cancel
      }
    }
  }
  object TickRequestTracker {
    private sealed trait TickRequestState extends Product with Serializable
    private object TickRequestState {
      case object NotRequested extends TickRequestState
      final case class Requested(
          requestedTick: SynchronizerTimeTracker.TickRequest
      ) extends TickRequestState
      case object NotNeededAnyMore extends TickRequestState
    }

  }

  private sealed trait PendingTick {
    def desiredTimestamp: CantonTimestamp
    def hasBeenCancelled: Boolean
    def complete(): Unit
    def traceContext: TraceContext

    final val creationStackTrace: String = callStackForExecutedTimeProofRequest()
  }
  private object PendingTick {
    implicit val ordering: Ordering[PendingTick] = Ordering.by(_.desiredTimestamp)
  }

  private final class RequestingTick(
      override val desiredTimestamp: CantonTimestamp
  )(implicit override val traceContext: TraceContext)
      extends PendingTick
      with TickRequest {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    @volatile private var cancelled = false

    override def cancel(): Unit =
      cancelled = true

    def hasBeenCancelled: Boolean = cancelled

    override def complete(): Unit = ()
  }

  private final class AwaitingTick(override val desiredTimestamp: CantonTimestamp)(implicit
      override val traceContext: TraceContext
  ) extends PendingTick {
    val promise: Promise[Unit] = Promise[Unit]()

    def hasBeenCancelled: Boolean = false
    def complete(): Unit = promise.trySuccess(()).discard
  }

  /** Keep track of a value, and when we received said value, measured on the participant's clock */
  private final case class Received[+A](value: A, receivedAt: CantonTimestamp)

  /** Keep track of the latest value received and a promise to complete when the next one arrives It
    * is not a case class so that equality is object identity (equality on promises is anyway object
    * identity).
    */
  private final class LatestAndNext[A](
      val latest: Option[Received[A]],
      val next: Option[Promise[UnlessShutdown[A]]],
  ) {
    def withNextSet: LatestAndNext[A] =
      next.fold(LatestAndNext(latest, Promise[UnlessShutdown[A]]().some))(_ => this)
  }
  private object LatestAndNext {
    def apply[A](
        latest: Option[Received[A]],
        next: Option[Promise[UnlessShutdown[A]]],
    ): LatestAndNext[A] =
      new LatestAndNext(latest, next)
    def empty[A]: LatestAndNext[A] = LatestAndNext(None, None)
  }

  def apply(
      config: SynchronizerTimeTrackerConfig,
      clock: Clock,
      sequencerClient: SequencerClient,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SynchronizerTimeTracker =
    new SynchronizerTimeTracker(
      config,
      clock,
      TimeProofRequestSubmitter(
        config.timeRequest,
        clock,
        sequencerClient,
        timeouts,
        loggerFactory,
      ),
      timeouts,
      loggerFactory,
    )

  /** Controls whether we log the stack traces of all tick requests that lead to fetching a time
    * proof. Use this only for debugging purposes to identify the reason for the time proof
    * requests.
    */
  private val PrintCallStackForExecutedTimeProofRequests: Boolean = true

  @inline
  private def callStackForExecutedTimeProofRequest(): String =
    if (PrintCallStackForExecutedTimeProofRequests) {
      val callStack = Thread.currentThread().getStackTrace
      callStack
        // First entry is getStackTrace
        .drop(1)
        .dropWhile(_.getClassName.startsWith(classOf[SynchronizerTimeTracker].getName))
        .mkString("\n  ")
    } else ""
}
