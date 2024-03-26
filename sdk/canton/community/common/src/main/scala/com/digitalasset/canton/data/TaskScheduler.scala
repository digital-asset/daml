// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.metrics.api.MetricHandle.Counter
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.PeanoQueue.{BeforeHead, InsertedValue, NotInserted}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, SimpleExecutionQueue}
import com.digitalasset.canton.{DiscardOps, SequencerCounter, SequencerCounterDiscriminator}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
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
class TaskScheduler[Task <: TaskScheduler.TimedTask](
    initSc: SequencerCounter,
    initTimestamp: CantonTimestamp,
    equalTimestampTaskOrdering: Ordering[Task],
    metrics: TaskSchedulerMetrics,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  /** Stores the timestamp up to which all tasks are known and can be performed,
    * unless they cannot be completed right now.
    */
  private[this] val latestPolledTimestamp: AtomicReference[CantonTimestamp] = new AtomicReference(
    initTimestamp
  )

  /** Contains all the scheduled tasks (activeness check/timeout/finalization) in the order
    * in which they must be performed.
    *
    * Since [[scala.collection.mutable.PriorityQueue]] is a max priority queue,
    * but we conceptually need a min priority queue, we reverse the order
    *
    * Invariant: contains only timestamps equal to or higher than [[latestPolledTimestamp]],
    * except if the first entry is a task that could not be completed.
    */
  private[this] val taskQueue: mutable.PriorityQueue[Traced[Task]] = mutable.PriorityQueue()(
    Ordering
      .by[Traced[Task], (CantonTimestamp, Task)](task => (task.value.timestamp, task.value))(
        Ordering.Tuple2(Ordering.ordered, equalTimestampTaskOrdering)
      )
      .reverse
  )

  /** Keeps track of all sequence counters and their associated timestamps.
    *
    * Invariant for public methods: The head is always the front. Timestamps strictly increase with sequencer counters.
    */
  private[this] val sequencerCounterQueue: PeanoQueue[SequencerCounter, CantonTimestamp] =
    new PeanoTreeQueue[SequencerCounterDiscriminator, CantonTimestamp](initSc)

  /** Contains all the time barriers in the order in which they must be signalled.
    *
    * Since [[scala.collection.mutable.PriorityQueue]] is a max priority queue,
    * we reverse the order as we need a min priority queue.
    *
    * When a barrier is removed from [[barrierQueue]], its promise completes.
    *
    * Invariant: contains only timestamps higher than [[latestPolledTimestamp]].
    */
  private[this] val barrierQueue: mutable.PriorityQueue[TaskScheduler.TimeBarrier] =
    mutable.PriorityQueue()(
      Ordering.by[TaskScheduler.TimeBarrier, CantonTimestamp](_.timestamp).reverse
    )

  /** The queue controlling the sequential execution of tasks within the scheduler.
    */
  private[this] val queue: SimpleExecutionQueue =
    new SimpleExecutionQueue(
      "task-scheduler",
      futureSupervisor,
      timeouts,
      loggerFactory,
      logTaskTiming = true,
    )

  private[this] val lock: Object = new Object

  // init metrics
  private val queueSizeGauge: CloseableGauge = metrics.taskQueue(() => taskQueue.size)

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
  def scheduleTask(task: Task)(implicit traceContext: TraceContext): Unit = blocking {
    lock.synchronized {
      if (task.timestamp < latestPolledTimestamp.get) {
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"Timestamp ${task.timestamp} of new task $task lies before current time $latestPolledTimestamp."
          )
        )
      }
      ErrorUtil.requireArgument(
        task.sequencerCounter >= sequencerCounterQueue.head,
        s"Sequencer counter already processed; head is at ${sequencerCounterQueue.head}, task is ${task}",
      )
      logger.trace(s"Adding task $task to the task scheduler.")
      taskQueue.enqueue(Traced(task))
    }
  }

  /** Schedules a new barrier at the given timestamp.
    *
    * @return A future that completes when all sequencer counters up to the given timestamp have been signalled.
    *         [[scala.None$]] if all sequencer counters up to the given timestamp have already been signalled.
    */
  def scheduleBarrier(timestamp: CantonTimestamp): Option[Future[Unit]] = blocking {
    lock.synchronized {
      if (latestPolledTimestamp.get >= timestamp) None
      else {
        val barrier = TaskScheduler.TimeBarrier(timestamp)
        barrierQueue.enqueue(barrier)
        Some(barrier.completion.future)
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

      performActionsAndCompleteBarriers()
    }
  }

  /** The returned future completes after all tasks that can be currently performed have completed. Never fails. */
  @VisibleForTesting
  def flush(): Future[Unit] = queue.flush()

  override def onClosed(): Unit =
    Lifecycle.close(queueSizeGauge, queue)(logger)

  /** Chains the futures of all actions whose timestamps the request tracker can progress to.
    *
    * @throws java.lang.IllegalStateException if non-monotonic timestamps for sequencer counters are found
    */
  private[this] def performActionsAndCompleteBarriers()(implicit
      traceContext: TraceContext
  ): Unit = {
    // drain the sequencerCounterQueue and record the latest observed timestamp
    @tailrec def pollAll(): Unit = {
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
    }
    pollAll()

    val _ = performUnlessClosing(functionFullName) {
      val observedTime = latestPolledTimestamp.get
      completeBarriersUpTo(observedTime)
      performActionsUpto(observedTime)
    }
  }

  /** Takes actions out of the `taskQueue` and processes them immediately until the next task has a timestamp
    * larger than `observedTime`.
    */
  private[this] def performActionsUpto(observedTime: CantonTimestamp): Unit = {
    @tailrec def go(): Unit = taskQueue.headOption match {
      case None => ()
      case Some(tracedTask) if tracedTask.value.timestamp > observedTime => ()
      case Some(tracedTask) =>
        tracedTask.withTraceContext { implicit traceContext => task =>
          FutureUtil.doNotAwait(
            // Close the task if the queue is shutdown or if it has failed
            queue
              .executeUS(task.perform(), task.toString)
              .onShutdown(task.close())
              .recoverWith {
                // If any task fails, none of subsequent tasks will be executed so we might as well close the scheduler
                // to force completion of the tasks and signal that the scheduler is not functional
                case NonFatal(e) if !this.isClosing =>
                  this.close()
                  Future.failed(e)
                // Use a direct context here to avoid closing the scheduler in a different thread
              }(DirectExecutionContext(errorLoggingContext(traceContext).noTracingLogger)),
            show"A task failed with an exception.\n$task",
          )
          taskQueue.dequeue()
        }.discard
        go()
    }

    go()
  }

  private[this] def completeBarriersUpTo(observedTime: CantonTimestamp): Unit = {
    @tailrec def go(): Unit = barrierQueue.headOption match {
      case None => ()
      case Some(barrier) if barrier.timestamp > observedTime => ()
      case Some(barrier) =>
        barrier.completion.success(())
        barrierQueue.dequeue().discard
        go()
    }

    go()
  }
}

trait TaskSchedulerMetrics {
  def sequencerCounterQueue: Counter
  def taskQueue(size: () => Int): CloseableGauge
}

object TaskScheduler {

  final case class TimeBarrier(timestamp: CantonTimestamp) {
    private[TaskScheduler] val completion: Promise[Unit] = Promise[Unit]()
  }
  trait TimedTask extends PrettyPrinting with AutoCloseable {

    /** The timestamp when the task should be scheduled */
    def timestamp: CantonTimestamp

    /** The sequencer counter that triggers this task */
    def sequencerCounter: SequencerCounter

    /** Perform the task. The future completes when the task is completed */
    def perform(): FutureUnlessShutdown[Unit]
  }
}
