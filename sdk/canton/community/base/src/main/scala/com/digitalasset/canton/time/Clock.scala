// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.data.EitherT
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{ClientConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ClockErrorGroup
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle, UnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.time.Clock.SystemClockRunningBackwards
import com.digitalasset.canton.topology.admin.v0.InitializationServiceGrpc
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, PriorityBlockingQueueUtil}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.empty.Empty

import java.time.{Clock as JClock, Duration, Instant}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{Callable, PriorityBlockingQueue, TimeUnit}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Random, Try}

/** A clock returning the current time, but with a twist: it always
  * returns unique timestamps. If two calls are made to the same clock
  * instance at the same time (according to the resolution of this
  * clock), one of the calls will block, until it can return a unique
  * value.
  *
  * All public functions are thread-safe.
  */
abstract class Clock() extends TimeProvider with AutoCloseable with NamedLogging {

  protected val last = new AtomicReference[CantonTimestamp](CantonTimestamp.Epoch)
  private val backwardsClockAlerted = new AtomicReference[CantonTimestamp](CantonTimestamp.Epoch)

  /** Potentially non-monotonic system clock
    *
    * Never use Instant.now, use the clock (as we also support sim-clock). If you need to ensure
    * that the clock is monotonically increasing, use the [[uniqueTime]] method instead.
    */
  def now: CantonTimestamp

  def isSimClock: Boolean = this match {
    case _: SimClock => true
    case _ => false
  }
  protected def warnIfClockRunsBackwards: Boolean = false

  protected case class Queued(action: CantonTimestamp => Unit, timestamp: CantonTimestamp) {

    val promise = Promise[UnlessShutdown[Unit]]()

    def run(now: CantonTimestamp): Unit =
      promise.complete(Try(UnlessShutdown.Outcome(action(now))))

  }

  protected def addToQueue(queue: Queued): Unit

  /** thread safe weakly monotonic time: each timestamp will be either equal or increasing
    * May go backwards across restarts.
    */
  final def monotonicTime(): CantonTimestamp = internalMonotonicTime(0)

  /** thread safe strongly monotonic increasing time: each timestamp will be unique
    * May go backwards across restarts.
    */
  final def uniqueTime(): CantonTimestamp = internalMonotonicTime(1)

  private def internalMonotonicTime(minSpacingMicros: Long): CantonTimestamp = {
    // `now` may block, so we call it before entering the `updateAndGet` block below.
    val nowSnapshot = now
    last.updateAndGet { oldTs =>
      if (oldTs.isBefore(nowSnapshot))
        nowSnapshot
      else {
        // emit warning if clock is running backwards
        if (
          warnIfClockRunsBackwards && // turn of warning for simclock, as access to now and last might be racy
          oldTs.isAfter(nowSnapshot.plusSeconds(1L)) && backwardsClockAlerted
            .get()
            .isBefore(nowSnapshot.minusSeconds(30))
        ) {
          import TraceContext.Implicits.Empty.*
          backwardsClockAlerted.set(nowSnapshot)
          SystemClockRunningBackwards.Error(nowSnapshot, oldTs).discard
        }
        if (minSpacingMicros > 0)
          oldTs.addMicros(minSpacingMicros)
        else oldTs
      }
    }
  }

  protected val tasks =
    new PriorityBlockingQueue[Queued](
      PriorityBlockingQueueUtil.DefaultInitialCapacity,
      (o1: Queued, o2: Queued) => o1.timestamp.compareTo(o2.timestamp),
    )

  /** Thread-safely schedule an action to be executed in the future
    *
    * If the provided `delta` is not positive the action skips queueing and is executed immediately.
    *
    * Same as other schedule method, except it expects a differential time amount
    */
  def scheduleAfter(
      action: CantonTimestamp => Unit,
      delta: Duration,
  ): FutureUnlessShutdown[Unit] =
    scheduleAt(action, now.add(delta))

  /** Thread-safely schedule an action to be executed in the future
    * actions need not execute in the order of their timestamps.
    *
    * If the provided timestamp is before `now`, the action skips queueing and is executed immediately.
    *
    * @param action action to run at the given timestamp (passing in the timestamp for when the task was scheduled)
    * @param timestamp timestamp when to run the task
    * @return a future for the given task
    */
  def scheduleAt(
      action: CantonTimestamp => Unit,
      timestamp: CantonTimestamp,
  ): FutureUnlessShutdown[Unit] = {
    val queued = Queued(action, timestamp)
    if (!now.isBefore(timestamp)) {
      queued.run(now)
    } else {
      addToQueue(queued)
    }
    FutureUnlessShutdown(queued.promise.future)
  }

  // flush the task queue, stopping once we hit a task in the future
  @tailrec
  private def doFlush(): Option[CantonTimestamp] = {
    val queued = Option(tasks.poll())
    queued match {
      // if no task present, do nothing
      case None => None
      case Some(item) =>
        // if task is present but in the future, put it back
        val currentTime = now
        if (item.timestamp > currentTime) {
          tasks.add(item)
          // If the clock was advanced concurrently while this call was checking the task's time against now
          // then the task will not `run` until the next call to `flush`. So if we see that the time was advanced
          // rerun `flush()`.
          if (now >= item.timestamp) doFlush()
          else Some(item.timestamp)
        } else {
          // otherwise execute task and iterate
          item.run(currentTime)
          doFlush()
        }
    }
  }

  protected def flush(): Option[CantonTimestamp] = doFlush()

  protected def failTasks(): Unit = {
    @tailrec def go(): Unit =
      Option(tasks.poll()) match {
        case None => ()
        case Some(item) =>
          item.promise.success(AbortedDueToShutdown)
          go()
      }
    go()
  }

  override def nowInMicrosecondsSinceEpoch: Long = uniqueTime().underlying.micros
}

object Clock extends ClockErrorGroup {

  @Explanation("""This error is emitted if the unique time generation detects that the host system clock is lagging behind
      |the unique time source by more than a second. This can occur if the system processes more than 2e6 events per second (unlikely)
      |or when the underlying host system clock is running backwards.""")
  @Resolution(
    """Inspect your host system. Generally, the unique time source is not negatively affected by a clock moving backwards
      |and will keep functioning. Therefore, this message is just a warning about something strange being detected."""
  )
  object SystemClockRunningBackwards
      extends ErrorCode(
        id = "SYSTEM_CLOCK_RUNNING_BACKWARDS",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {
    final case class Error(now: CantonTimestamp, oldUniqueTime: CantonTimestamp)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            show"WallClock's system clock seems to be moving backwards: now=$now vs uniqueTs=$oldUniqueTime",
          throwableO = None,
        )
  }

}

trait TickTock {
  def now: Instant
}

object TickTock {
  object Native extends TickTock {
    private val jclock = JClock.systemUTC()
    def now: Instant = jclock.instant()
  }
  class RandomSkew(maxSkewMillis: Int) extends TickTock {

    private val changeSkewMillis = Math.max(maxSkewMillis / 10, 1)

    private val jclock = JClock.systemUTC()
    private val random = new Random(0)
    private val skew = new AtomicLong(
      (random.nextInt(2 * maxSkewMillis + 1) - maxSkewMillis).toLong
    )
    private val last = new AtomicLong(0)

    private def updateSkew(current: Long): Long = {
      val upd = random.nextInt(2 * changeSkewMillis + 1) - changeSkewMillis
      val next = current + upd
      if (next > maxSkewMillis) maxSkewMillis.toLong
      else if (next < -maxSkewMillis) -maxSkewMillis.toLong
      else next
    }

    private def updateLast(current: Long): Long = {
      val nextSkew = skew.updateAndGet(updateSkew)
      val instant = jclock.instant().toEpochMilli
      Math.max(instant + nextSkew, current + 1)
    }

    def now: Instant = Instant.ofEpochMilli(last.updateAndGet(updateLast))

  }
}

class WallClock(
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    tickTock: TickTock = TickTock.Native,
) extends Clock
    with NamedLogging {

  last.set(CantonTimestamp.assertFromInstant(tickTock.now))

  def now: CantonTimestamp = CantonTimestamp.assertFromInstant(tickTock.now)
  override protected def warnIfClockRunsBackwards: Boolean = true

  // Keeping a dedicated scheduler, as it may have to run long running tasks.
  // Once all tasks are guaranteed to be "light", the environment scheduler can be used.
  private val scheduler = Threading.singleThreadScheduledExecutor(
    loggerFactory.threadName + "-wallclock",
    noTracingLogger,
  )
  private val running = new AtomicBoolean(true)

  override def close(): Unit = {
    import com.digitalasset.canton.concurrent.*
    if (running.getAndSet(false)) {
      Lifecycle.close(
        () => failTasks(),
        () => ExecutorServiceExtensions(scheduler)(logger, timeouts).close("clock"),
      )(logger)
    }
  }

  override protected def addToQueue(queued: Queued): Unit = {
    val head = Option(tasks.peek())
    val scheduleNew = head match {
      case Some(item) => item.timestamp.isAfter(queued.timestamp)
      case None => true
    }
    tasks.add(queued)
    if (scheduleNew) {
      scheduleNext(queued.timestamp)
    }
  }

  private val nextFlush = new AtomicReference[Option[CantonTimestamp]](None)
  // will schedule a new flush at the given time
  private def scheduleNext(timestamp: CantonTimestamp): Unit = {
    if (running.get()) {
      // update next flush reference if this timestamp is before the current scheduled
      val newTimestamp = Some(timestamp)
      def updateCondition(current: Option[CantonTimestamp]): Boolean =
        current.forall(_ > timestamp)
      val current = nextFlush.getAndUpdate { old =>
        if (updateCondition(old)) newTimestamp else old
      }
      val needsSchedule = updateCondition(current)
      if (needsSchedule) {
        // add one ms as we will process all tasks up to now() which means that if we use ms precision,
        // we need to set it to the next ms such that we include all tasks within the same ms
        val delta = Math.max(Duration.between(now.toInstant, timestamp.toInstant).toMillis, 1) + 1
        val _ = scheduler.schedule(
          new Runnable() {
            override def run(): Unit = {
              // mark that this flush has been executed before starting the flush,
              // (if something else is queued after our flush but before re-scheduling, it will
              // succeed in scheduling instead of this thread).
              // we only set it to None if nextFlush matches this one
              nextFlush.compareAndSet(newTimestamp, None)
              flush().foreach(scheduleNext)
            }
          },
          delta,
          TimeUnit.MILLISECONDS,
        )
      }
    }
  }

}

class SimClock(
    start: CantonTimestamp = CantonTimestamp.Epoch,
    val loggerFactory: NamedLoggerFactory,
) extends Clock
    with NamedLogging {

  private val value = new AtomicReference[CantonTimestamp](start)
  last.set(start)

  def now: CantonTimestamp = value.get()

  override def flush(): Option[CantonTimestamp] = super.flush()

  def advanceTo(timestamp: CantonTimestamp, doFlush: Boolean = true)(implicit
      traceContext: TraceContext
  ): Unit = {
    ErrorUtil.requireArgument(
      now.isBefore(timestamp) || now == timestamp,
      s"advanceTo failed with time going backwards: current timestamp is $now and request is $timestamp",
    )
    logger.info(s"Advancing sim clock to $timestamp")
    value.updateAndGet(_.max(timestamp))
    if (doFlush) {
      flush().discard[Option[CantonTimestamp]]
    }
  }

  def advance(duration: Duration)(implicit traceContext: TraceContext): Unit = {
    ErrorUtil.requireArgument(!duration.isNegative, show"Duration must not be negative: $duration.")
    logger.info(s"Advancing sim clock by $duration")
    value.updateAndGet(_.add(duration))
    flush().discard[Option[CantonTimestamp]]
  }

  override def close(): Unit = {}

  override protected def addToQueue(queue: Queued): Unit = {
    val _ = tasks.add(queue)
  }

  def reset(): Unit = {
    failTasks()
    value.set(start)
    last.set(start)
  }

  override def toString: String = s"SimClock($now)"

  @VisibleForTesting
  def numberOfScheduledTasks: Int = tasks.size()
}

class RemoteClock(
    config: ClientConfig,
    timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContextExecutor)
    extends Clock
    with NamedLogging {

  // same as wall-clock: we might use the normal execution context if the tasks are guaranteed to be light
  private val scheduler = Threading.singleThreadScheduledExecutor(
    loggerFactory.threadName + "-remoteclock",
    noTracingLogger,
  )
  private val channel = ClientChannelBuilder.createChannelToTrustedServer(config)
  private val service = InitializationServiceGrpc.stub(channel)
  private val running = new AtomicBoolean(true)
  private val updating = new AtomicReference[Option[CantonTimestamp]](None)

  backgroundUpdate()

  private def backgroundUpdate(): Unit = {
    if (running.get()) {
      update().discard

      val _ = scheduler.schedule(
        new Callable[Unit] {
          override def call(): Unit = backgroundUpdate()
        },
        500,
        TimeUnit.MILLISECONDS,
      )
    }
  }

  private def update(): CantonTimestamp = {
    // the update method is invoked on every call to now()
    // in a remote sim-clock setting, we don't know when we need to flush()
    // therefore, we try to flush whenever a timestamp is requested.
    // however, we need to avoid recursion if `clock.now` is invoked from within the flush()
    // therefore, we use an atomic reference to lock the access to the flush and while
    // the system is flushing, we just keep on returning the stored timestamp
    val ret = updating.get() match {
      // on an access to `now` while we are updating, just return the cached timestamp
      case Some(tm) => tm
      case None =>
        // fetch current timestamp
        val tm = getRemoteTime
        // see if something has been racing with us. if so, use other timestamp
        updating.getAndUpdate {
          case None => Some(tm)
          case Some(racyTm) => Some(racyTm)
        } match {
          // no race, flush!
          case None =>
            flush().discard
            updating.set(None)
            tm
          // on a race, return racy timestamp
          case Some(racyTm) => racyTm
        }
    }
    ret
  }

  override def now: CantonTimestamp = update()

  @tailrec
  private def getRemoteTime: CantonTimestamp = {
    val req = for {
      pbTimestamp <- EitherT.right[ProtoDeserializationError](service.currentTime(Empty()))
      timestamp <- EitherT.fromEither[Future](CantonTimestamp.fromProtoPrimitive(pbTimestamp))
    } yield timestamp
    import TraceContext.Implicits.Empty.*
    timeouts.network.await("fetching remote time")(req.value) match {
      case Right(tm) => tm
      case Left(err) =>
        // we are forgiving here. a background process might start faster than the foreground process
        // so the grpc call might fail because the API is not online. but as we are doing testing only,
        // we don't make a big fuss about it, just emit a log and retry
        noTracingLogger.info(
          s"Failed to fetch remote time from ${config.port.unwrap}: ${err}. Will try again"
        )
        Threading.sleep(500)
        getRemoteTime
    }
  }

  override protected def addToQueue(queue: Queued): Unit = {
    val _ = tasks.add(queue)
  }

  override def close(): Unit =
    if (running.getAndSet(false)) {
      // stopping the scheduler before the channel, so we don't get a failed call on shutdown
      scheduler.shutdown()
      Lifecycle.toCloseableChannel(channel, logger, "channel to remote clock server").close()
    }
}

/** This implementation allows us to control many independent sim clocks at the same time.
  * Possible race conditions that might happen with concurrent start/stop of clocks are not
  * being addressed but are currently unlikely to happen.
  * @param currentClocks a function that returns all current running sim clocks
  * @param start start time of this clock
  */
class DelegatingSimClock(
    currentClocks: () => Seq[SimClock],
    val start: CantonTimestamp = CantonTimestamp.Epoch,
    loggerFactory: NamedLoggerFactory,
) extends SimClock(start, loggerFactory) {

  override def advanceTo(timestamp: CantonTimestamp, doFlush: Boolean = true)(implicit
      traceContext: TraceContext
  ): Unit = ErrorUtil.withThrowableLogging {
    super.advanceTo(timestamp, doFlush)
    currentClocks().foreach(_.advanceTo(now, doFlush = false))
    // avoid race conditions between nodes by first adjusting the time and then flushing the tasks
    if (doFlush)
      currentClocks().foreach(_.flush().discard)
  }

  override def advance(duration: Duration)(implicit traceContext: TraceContext): Unit =
    ErrorUtil.withThrowableLogging {
      super.advance(duration)
      // avoid race conditions between nodes by first adjusting the time and then flushing the tasks
      currentClocks().foreach(_.advanceTo(now, doFlush = false))
      currentClocks().foreach(_.flush().discard)
    }

  override def close(): Unit = {
    super.close()
    currentClocks().foreach(_.close())
  }

  override def reset(): Unit = {
    super.reset()
    currentClocks().foreach(_.reset())
  }
}
