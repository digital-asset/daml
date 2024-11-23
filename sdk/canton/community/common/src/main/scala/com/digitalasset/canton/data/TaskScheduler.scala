// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.metrics.api.MetricHandle.Counter
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.PeanoQueue.{BeforeHead, InsertedValue, NotInserted}
import com.digitalasset.canton.data.TaskScheduler.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{
  ErrorUtil,
  FutureUnlessShutdownUtil,
  FutureUtil,
  SimpleExecutionQueue,
}
import com.digitalasset.canton.{SequencerCounter, SequencerCounterDiscriminator}
import com.google.common.annotations.VisibleForTesting

import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.language.implicitConversions
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.control.NonFatal

/** The task scheduler manages tasks with associated timestamps and sequencer counters.
  * Tasks may be inserted in any order; they will be executed nevertheless in the correct order
  * given by the timestamps.
  *
  * The tasks execute sequentially in [[scala.concurrent.Future]].
  *
  * @param initSc The first sequencer counter to be processed
  * @param initTimestamp Only timestamps after this timestamp can be used
  * @param equalTimestampTaskOrdering The ordering for tasks with the same timestamps;
  *                                   tasks that are smaller w.r.t this order are processed earlier.
  */
class TaskScheduler[Task <: TimedTask](
    initSc: SequencerCounter,
    initTimestamp: CantonTimestamp,
    alertAfter: JDuration,
    alertEvery: JDuration,
    equalTimestampTaskOrdering: Ordering[Task],
    metrics: TaskSchedulerMetrics,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    clock: Clock,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  /** Stores the timestamp up to which all tasks are known and can be performed,
    * unless they cannot be completed right now.
    */
  private[this] val latestPolledTimestamp: AtomicReference[CantonTimestamp] = new AtomicReference(
    initTimestamp
  )

  private type BarrierOrTask = Either[TimeBarrierTask, Task]
  private implicit def toTimedTask(barrierOrTask: BarrierOrTask): TimedTask = barrierOrTask.merge
  private implicit val taskOrdering: Ordering[Task] = equalTimestampTaskOrdering
  // barrier comes first for the same timestamp
  private implicit val barrierOrTaskOrdering: Ordering[BarrierOrTask] =
    Ordering
      .by[BarrierOrTask, (CantonTimestamp, Option[Task])](task => task.timestamp -> task.toOption)

  /** Contains all the scheduled tasks (activeness check/timeout/finalization) in the order
    * in which they must be performed.
    *
    * Since [[scala.collection.mutable.PriorityQueue]] is a max priority queue,
    * but we conceptually need a min priority queue, we reverse the order
    *
    * Invariant: contains only timestamps equal to or higher than [[latestPolledTimestamp]],
    * except if the first entry is a task that could not be completed.
    */
  private[this] val taskQueue: mutable.PriorityQueue[BarrierOrTask] =
    mutable.PriorityQueue()(Ordering[BarrierOrTask].reverse)

  /** Keeps track of all sequence counters and their associated timestamps.
    *
    * Invariant for public methods: The head is always the front. Timestamps strictly increase with sequencer counters.
    */
  private[this] val sequencerCounterQueue: PeanoQueue[SequencerCounter, CantonTimestamp] =
    new PeanoTreeQueue[SequencerCounterDiscriminator, CantonTimestamp](initSc)

  /** The queue controlling the sequential execution of tasks within the scheduler.
    */
  private[this] val queue: SimpleExecutionQueue =
    new SimpleExecutionQueue(
      "task-scheduler",
      futureSupervisor,
      timeouts,
      loggerFactory,
      logTaskTiming = true,
      crashOnFailure = exitOnFatalFailures,
    )

  private[this] val lock: Object = new Object

  // init metrics
  private val queueSizeGauge: CloseableGauge = metrics.taskQueue(() => taskQueue.size)

  /** The sequencer counter that has last been ticked *and* thereby advanced sequencerCounterQueue.front.
    * The timestamp corresponds to `clock.now` at the time of the tick.
    */
  private val lastProgress: AtomicReference[(SequencerCounter, CantonTimestamp)] =
    new AtomicReference((initSc - 1) -> clock.now)

  /** The highest sequencer timestamp that has ever been ticked. */
  private val highWatermark: AtomicReference[CantonTimestamp] =
    new AtomicReference(initTimestamp)

  scheduleNextCheck(alertAfter)

  private def scheduleNextCheck(after: JDuration): Unit =
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      clock.scheduleAfter(_ => checkIfBlocked(), after),
      "The check for missing ticks has failed unexpectedly",
    )(errorLoggingContext(TraceContext.empty))

  private def checkIfBlocked(): Unit = {
    implicit val empty: TraceContext = TraceContext.empty
    performUnlessClosing("check for missing ticks") {

      val now = clock.now
      val (sc, lastTick) = lastProgress.get()
      val noProgressDuration = now - lastTick

      if (noProgressDuration >= alertAfter) {
        val highWatermarkTs = highWatermark.get()

        val blocked = blocking {
          lock.synchronized {
            if (taskQueue.headOption.exists(_.timestamp <= highWatermarkTs))
              taskQueue
                .filter(_.timestamp <= highWatermarkTs)
                .map(_.traceContext.traceId.getOrElse(""))
                .toSet
            else {
              // If there is no blocked task, we do not need to traverse the entire queue.
              Set.empty
            }
          }
        }
        if (blocked.nonEmpty) {
          logger.info(
            s"Task scheduler waits for tick of sc=${sc + 1}. The tick with sc=$sc occurred at $lastTick. " +
              s"Blocked trace ids: ${blocked.mkString(", ")}"
          )
        }
        scheduleNextCheck(alertEvery)
      } else {
        scheduleNextCheck(alertAfter minus noProgressDuration)
      }
    }.onShutdown(logger.debug("Stop periodic check for missing ticks."))
  }

  /** Used to inspect the state of the sequencerCounterQueue, for testing purposes. */
  @VisibleForTesting
  def readSequencerCounterQueue: SequencerCounter => PeanoQueue.AssociatedValue[CantonTimestamp] =
    sequencerCounterQueue.get

  /** Adds a new task to be executed at the given timestamp and with the associated sequencer counter.
    * This method does not register the timestamp as being observed.
    * So [[addTick]] must be called separately if desired.
    *
    * @param task The task to execute.
    * @throws java.lang.IllegalArgumentException
    *         if the `timestamp` or `sequencer counter` of the task is earlier
    *         than to where the task scheduler has already progressed
    */
  def scheduleTask(task: Task): Unit = blocking {
    lock.synchronized {
      implicit val traceContext: TraceContext = task.traceContext
      if (task.timestamp < latestPolledTimestamp.get) {
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"Timestamp ${task.timestamp} of new task $task lies before current time $latestPolledTimestamp."
          )
        )
      }
      task match {
        case timedTaskWithSequencerCounter: TimedTaskWithSequencerCounter =>
          ErrorUtil.requireArgument(
            timedTaskWithSequencerCounter.sequencerCounter >= sequencerCounterQueue.head,
            s"Sequencer counter already processed; head is at ${sequencerCounterQueue.head}, task is $task",
          )

        case _ => ()
      }
      logger.trace(s"Adding task $task to the task scheduler.")
      taskQueue.enqueue(Right(task))
    }
  }

  /** Schedule task only, if the desired timestamp is after the current latest polled timestamp.
    *
    * @param taskFactory The function creating a task from the desired timestamp.
    *                    This function will immediately execute, and the resulting task will be scheduled to the queue
    * @return A Left with the latest timestamp, if it is after the desired timestamp, or a Right with the created Task itself.
    */
  def scheduleTaskIfLater[T <: Task](
      desiredTimestamp: CantonTimestamp,
      taskFactory: CantonTimestamp => T,
  ): Either[CantonTimestamp, T] = blocking {
    lock.synchronized {
      val polledTimestamp = latestPolledTimestamp.get
      if (desiredTimestamp <= polledTimestamp) {
        Left(polledTimestamp)
      } else {
        val task = taskFactory(desiredTimestamp)
        scheduleTask(task)
        Right(task)
      }
    }
  }

  /** Scheduling a task immediately.
    * This does not mean it will be executed immediately: all preceding tasks will finish executing first.
    *
    * @param taskFactory The function which will return the async result at execution. The input will be the realized
    *                    timestamp.
    * @return The realized timestamp.
    */
  def scheduleTaskImmediately(
      taskFactory: CantonTimestamp => FutureUnlessShutdown[Unit],
      taskTraceContext: TraceContext,
  ): CantonTimestamp = blocking {
    lock.synchronized {
      val currentTimestamp = latestPolledTimestamp.get()
      implicit val traceContext: TraceContext = taskTraceContext
      logger.trace(s"Adding task to the task scheduler immediately ($currentTimestamp).")
      executeTask(
        new ImmediateTask(
          timestamp = currentTimestamp,
          traceContext = taskTraceContext,
          performFUS = () => taskFactory(currentTimestamp),
        )
      )
      currentTimestamp
    }
  }

  /** Schedules a new barrier at the given timestamp.
    *
    * @return A future that completes when all sequencer counters up to the given timestamp have been signalled.
    *         [[scala.None$]] if all sequencer counters up to the given timestamp have already been signalled.
    */
  def scheduleBarrierUS(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Option[FutureUnlessShutdown[Unit]] = blocking {
    lock.synchronized {
      if (latestPolledTimestamp.get >= timestamp) None
      else {
        val barrierPromise = mkPromise[Unit]("task-scheduler-time-barrier", futureSupervisor)
        val barrier = TaskScheduler.TimeBarrierTask(
          timestamp,
          () =>
            performUnlessClosingF(functionFullName)(
              Future.successful(
                barrierPromise.outcome(())
              )
            ).tapOnShutdown(
              // the barrierPromise will close anyway eventually, this is just to prevent races
              barrierPromise.shutdown()
            ),
        )
        taskQueue.enqueue(Left(barrier))
        Some(barrierPromise.futureUS)
      }
    }
  }

  /** Signals that the sequence counter with the given timestamp has been observed.
    *
    * Eventually, all sequencer counters above `initSc` must be added with their timestamp using this method.
    * Every sequencer counter must be added once and timestamps must strictly increase with
    * sequencer counters.
    *
    * If all sequencer counters between `initSc` and `sequencerCounter` have been added,
    * then the tasks up to `timestamp` will be performed, unless there is a task that could not complete.
    * In that case, task processing stops with the unfinished task.
    *
    * @see TaskScheduler.runTasks()
    *
    * @throws java.lang.IllegalArgumentException
    *         <ul>
    *           <li>If the `sequencerCounter` has not been inserted,
    *               but all sequencer counters up to `timestamp` have been inserted.</li>
    *           <li>If the `sequencerCounter` is the first sequencer counter to be processed
    *               and the `timestamp` is not after the timestamp given to the constructor.</li>
    *           <li>If the `sequencerCounter` has been inserted with a different timestamp.</li>
    *           <li>If the `timestamp` is at most the timestamp of a smaller sequencer counter,
    *               or if the `timestamp` is at least the timestamp of a larger sequencer counter.</li>
    *           <li>If the `sequencerCounter` is `Long.MaxValue`.</li>
    *         </ul>
    */
  def addTick(sequencerCounter: SequencerCounter, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit = blocking {
    // We lock the whole method here because the priority queue and the peano queue are not thread-safe.
    lock.synchronized {
      logger.trace(
        s"Signalling sequencer counter $sequencerCounter at $timestamp to the task scheduler. Head is ${sequencerCounterQueue.head}"
      )
      ErrorUtil.requireArgument(
        sequencerCounter.isNotMaxValue,
        "Sequencer counter Long.MaxValue signalled to task scheduler.",
      )

      // We lock the whole method here
      val latestTime = latestPolledTimestamp.get

      if (timestamp <= latestTime && sequencerCounter >= sequencerCounterQueue.front) {
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"Timestamp $timestamp for sequence counter $sequencerCounter is not after current time $latestPolledTimestamp."
          )
        )
      }

      sequencerCounterQueue.get(sequencerCounter) match {
        case NotInserted(floor, ceiling) =>
          floor match {
            case Some(ts) if ts >= timestamp =>
              ErrorUtil.internalError(
                new IllegalArgumentException(
                  s"Timestamp $timestamp for sequencer counter $sequencerCounter is not after timestamp $ts of an earlier sequencer counter."
                )
              )
            case _ =>
          }
          ceiling match {
            case Some(ts) if ts <= timestamp =>
              ErrorUtil.internalError(
                new IllegalArgumentException(
                  s"Timestamp $timestamp for sequencer counter $sequencerCounter is not before timestamp $ts of a later sequencer counter."
                )
              )
            case _ =>
          }
          sequencerCounterQueue.insert(sequencerCounter, timestamp).discard
          metrics.sequencerCounterQueue.inc()
        case BeforeHead =>
          if (timestamp > latestTime)
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"Timestamp $timestamp for outdated sequencer counter $sequencerCounter is after current time $latestPolledTimestamp."
              )
            )
        case InsertedValue(oldTimestamp) =>
          if (oldTimestamp != timestamp)
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"Timestamp $timestamp for sequencer counter $sequencerCounter differs from timestamp $oldTimestamp that was signalled before."
              )
            )
      }

      val now = clock.now
      lastProgress.updateAndGet { lastState =>
        val (lastFront, _) = lastState
        val nextFront = sequencerCounterQueue.front - 1
        if (nextFront > lastFront) nextFront -> now
        else lastState
      }
      highWatermark.updateAndGet(_ max timestamp)

      performActionsAndCompleteBarriers()
    }
  }

  /** The returned future completes after all tasks that can be currently performed have completed. Never fails. */
  def flush(): Future[Unit] = queue.flush()

  override def onClosed(): Unit =
    LifeCycle.close(queueSizeGauge, queue)(logger)

  /** Chains the futures of all actions whose timestamps the request tracker can progress to.
    *
    * @throws java.lang.IllegalStateException if non-monotonic timestamps for sequencer counters are found
    */
  private[this] def performActionsAndCompleteBarriers()(implicit
      traceContext: TraceContext
  ): Unit = {
    // drain the sequencerCounterQueue and record the latest observed timestamp
    @tailrec def pollAll(): Unit =
      sequencerCounterQueue.poll() match {
        case None => ()
        case Some((sc, observedTime)) =>
          metrics.sequencerCounterQueue.dec()
          val previousTime = latestPolledTimestamp.getAndSet(observedTime)
          if (observedTime <= previousTime) {
            // This should never happen
            ErrorUtil.internalError(
              new IllegalStateException(
                s"Timestamp $observedTime for sequencer counter $sc is before current time $latestPolledTimestamp"
              )
            )
          }
          pollAll()
      }
    pollAll()

    val _ = performUnlessClosing(functionFullName) {
      val observedTime = latestPolledTimestamp.get
      performActionsUpto(observedTime)
    }
  }

  /** Takes actions out of the `taskQueue` and processes them immediately until the next task has a timestamp
    * larger than `observedTime`.
    */
  private[this] def performActionsUpto(observedTime: CantonTimestamp): Unit = {
    @tailrec def go(): Unit = taskQueue.headOption match {
      case None => ()
      case Some(task) if task.timestamp > observedTime => ()
      case Some(task) =>
        executeTask(task)
        taskQueue.dequeue().discard
        go()
    }

    go()
  }

  private def executeTask(task: TimedTask): Unit = {
    implicit val traceContext: TraceContext = task.traceContext
    performUnlessClosing(functionFullName) {
      FutureUtil.doNotAwait(
        // Close the task if the queue is shutdown or if it has failed
        queue
          .executeUS(
            futureSupervisor.supervisedUS(
              task.toString,
              timeouts.slowFutureWarn.duration,
            )(task.perform()),
            task.toString,
          )
          .onShutdown(task.close())
          .recoverWith {
            // If any task fails, none of subsequent tasks will be executed so we might as well close the scheduler
            // to force completion of the tasks and signal that the scheduler is not functional
            case NonFatal(e) if !this.isClosing =>
              this.close()
              Future.failed(e)
            // Use a direct context here to avoid closing the scheduler in a different thread
          }(DirectExecutionContext(noTracingLogger)),
        show"A task failed with an exception.\n$task",
      )
    }.discard
  }

}

trait TaskSchedulerMetrics {
  def sequencerCounterQueue: Counter
  def taskQueue(size: () => Int): CloseableGauge
}

object TaskScheduler {

  def apply[Task <: TaskScheduler.TimedTask](
      initSc: SequencerCounter,
      initTimestamp: CantonTimestamp,
      equalTimestampTaskOrdering: Ordering[Task],
      metrics: TaskSchedulerMetrics,
      exitOnFatalFailures: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
  )(implicit executionContext: ExecutionContext): TaskScheduler[Task] = new TaskScheduler[Task](
    initSc,
    initTimestamp,
    timeouts.slowFutureWarn.asJavaApproximation,
    timeouts.slowFutureWarn.asJavaApproximation,
    equalTimestampTaskOrdering,
    metrics,
    exitOnFatalFailures = exitOnFatalFailures,
    timeouts,
    loggerFactory,
    futureSupervisor,
    clock,
  )

  private final case class TimeBarrierTask(
      override val timestamp: CantonTimestamp,
      completeTimeBarrier: () => FutureUnlessShutdown[Unit],
  )(implicit val traceContext: TraceContext)
      extends TimedTask {

    override def perform(): FutureUnlessShutdown[Unit] =
      completeTimeBarrier()

    // no need to do anything as the PromisUS will complete automatically on shutdown
    override def close(): Unit = ()

    override protected def pretty: Pretty[TimeBarrierTask.this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp)
      )
  }

  private final class ImmediateTask(
      override val timestamp: CantonTimestamp,
      override val traceContext: TraceContext,
      performFUS: () => FutureUnlessShutdown[Unit],
  ) extends TimedTask {

    override def perform(): FutureUnlessShutdown[Unit] = performFUS()

    override def close(): Unit = ()

    override protected def pretty: Pretty[ImmediateTask.this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp)
      )
  }

  trait TimedTask extends HasTraceContext with PrettyPrinting with AutoCloseable {

    /** The timestamp when the instance should be executed/completed. */
    def timestamp: CantonTimestamp

    /** Perform the task. The future completes when the task is completed */
    def perform(): FutureUnlessShutdown[Unit]

  }

  trait TimedTaskWithSequencerCounter extends TimedTask {

    /** The sequencer counter that triggers this task */
    def sequencerCounter: SequencerCounter

  }
}
