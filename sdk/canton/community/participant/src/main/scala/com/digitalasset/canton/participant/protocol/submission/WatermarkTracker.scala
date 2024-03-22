// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.{FutureSupervisor, SupervisedPromise}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, Thereafter}
import com.google.common.annotations.VisibleForTesting

import java.util
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

trait WatermarkLookup[Mark] {

  /** Returns the current value of the watermark. */
  def highWatermark: Mark
}

/** Keeps track of a boundary [[WatermarkTracker.highWatermark]] that increases monotonically.
  * Clients can do one of the following:
  *
  * <ul>
  *   <li>Use the [[WatermarkTracker]] to execute a task associated with a given mark `mark` > [[WatermarkTracker.highWatermark]].
  *     If `mark` <= [[WatermarkTracker.highWatermark]], the task will be rejected.</li>
  *   <li>Use the tracker to increase the [[WatermarkTracker.highWatermark]].
  *     Notify the caller as soon as all tasks with `mark` <= [[WatermarkTracker.highWatermark]] have finished.</li>
  * </ul>
  *
  * The [[WatermarkTracker]] is effectively used to enforce mutual exclusion between two types of tasks.
  * <ol>
  *   <li>Tasks of type 1 act on data with `mark` > [[WatermarkTracker.highWatermark]].</li>
  *   <li>Tasks of type 2 act on data with `mark` <= [[WatermarkTracker.highWatermark]].</li>
  * </ul>
  *
  * @param initialWatermark The initial value for [[WatermarkTracker.highWatermark]]
  */
class WatermarkTracker[Mark: Pretty](
    initialWatermark: Mark,
    protected override val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit private val ordering: Ordering[Mark], ec: ExecutionContext)
    extends WatermarkLookup[Mark]
    with NamedLogging {
  import WatermarkTracker.*

  /** The high-watermark boundary, increases monotonically.
    *
    * Access must be guarded by [[lock]].
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var highWatermarkV: Mark = initialWatermark

  /** Associates marks to a promise.
    * The promise completes after there are no [[runningTasks]] at or below this mark.
    *
    * All keys are at or below [[highWatermark]].
    *
    * Access must be guarded by [[lock]].
    */
  private val waitForTasksFinishing: util.NavigableMap[Mark, Promise[Unit]] =
    new util.TreeMap[Mark, Promise[Unit]]()

  /** Counts the number of running tasks for a given mark.
    * New tasks are added only if their mark is above [[highWatermark]].
    *
    * Invariant: all values are positive integers.
    *
    * Access must be guarded by [[lock]].
    */
  private val runningTasks: util.NavigableMap[Mark, Int] = new util.TreeMap[Mark, Int]()

  /** Used to synchronize access to the mutable fields */
  // This data structure could probably be implemented without locks,
  // but it doesn't seem worth the effort for now.
  private val lock: AnyRef = new Object
  private def withLock[A](body: => A): A = blocking { lock.synchronized(body) }

  override def highWatermark: Mark = withLock { highWatermarkV }

  /** Run a task `task` if `mark` > [[highWatermark]].
    *
    * @return [[scala.Left$]] if `mark` is not higher than [[highWatermark]].
    *         Otherwise returns the result of running `task` as [[scala.Right$]].
    * @throws java.lang.IllegalStateException
    *   if there are already `Int.MaxValue` tasks running for `mark`
    */
  def runIfAboveWatermark[F[_], A](mark: Mark, register: => F[A])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      F: Thereafter[F],
  ): Either[MarkTooLow[Mark], F[A]] =
    registerBegin(mark).map { _ =>
      register.thereafter { _ =>
        checked(registerEnd(mark))
      }
    }

  /** Record that a task with mark `mark` wants to start.
    *
    * @return [[scala.Left$]] if `mark`` is not higher than [[highWatermark]].
    * @throws java.lang.IllegalStateException
    *   if there are already `Int.MaxValue` tasks with `mark` running.
    */
  @VisibleForTesting
  private[submission] def registerBegin(
      mark: Mark
  )(implicit traceContext: TraceContext): Either[MarkTooLow[Mark], Unit] =
    withLock {
      if (ordering.lteq(mark, highWatermarkV)) Left(MarkTooLow(highWatermarkV))
      else {
        val _ = Option(runningTasks.get(mark)) match {
          case None => runningTasks.put(mark, 1)
          case Some(count) =>
            ErrorUtil.requireState(
              count < Int.MaxValue,
              show"Overflow: already ${Int.MaxValue} running tasks for $mark.",
            )
            runningTasks.put(mark, count + 1)
        }
        Right(())
      }
    }

  /** Record that a task with mark `mark` has finished.
    *
    * @throws java.lang.IllegalStateException if there is no running task for `mark`
    */
  @VisibleForTesting
  private[submission] def registerEnd(mark: Mark)(implicit traceContext: TraceContext): Unit =
    withLock {
      val count = Option(runningTasks.get(mark)).getOrElse(
        ErrorUtil.internalError(new IllegalStateException(s"No running tasks for $mark"))
      )
      val _ =
        if (count == 1) runningTasks.remove(mark)
        else runningTasks.put(mark, count - 1)
      drainAndNotify()
    }

  /** Increases the [[highWatermark]] to `mark` unless it was higher previously.
    *
    * @return The future completes after there are no running tasks with marks up to `mark` inclusive.
    */
  def increaseWatermark(mark: Mark)(implicit
      traceContext: TraceContext
  ): Future[Unit] = withLock {
    highWatermarkV = ordering.max(highWatermarkV, mark)
    val promise = new SupervisedPromise[Unit]("increase-watermark", futureSupervisor)
    val previousO = Option(waitForTasksFinishing.put(mark, promise))
    previousO.foreach(_.completeWith(promise.future))
    drainAndNotify()
    promise.future
  }

  /** Remove all entries from [[waitForTasksFinishing]] and complete the associated promise
    * up to the first [[runningTasks]] timestamp exclusive (if it exists) or the [[highWatermarkV]] inclusive,
    * whatever is lower.
    *
    * The caller must have acquired [[lock]].
    */
  private def drainAndNotify(): Unit = {
    import scala.jdk.CollectionConverters.*
    val upto =
      // If there are no tasks or the first task's mark is higher than `highWatermark`,
      // we can complete all promises because all marks in `waitForTasksFinishing`
      // are at or below `highWatermark` by the invariant.
      if (runningTasks.isEmpty) waitForTasksFinishing
      else {
        val firstTaskMark = checked(runningTasks.firstKey)
        if (ordering.gt(firstTaskMark, highWatermarkV)) waitForTasksFinishing
        else waitForTasksFinishing.headMap(firstTaskMark, false)
      }

    for (promise <- upto.values.asScala) { promise.success(()) }
    upto.clear()
  }

}

object WatermarkTracker {

  /** The task's mark is lower than or equal to the high watermark */
  final case class MarkTooLow[A: Pretty](highWatermark: A) extends PrettyPrinting {
    override def pretty: Pretty[MarkTooLow.this.type] = prettyOfClass(
      param("high watermark", _.highWatermark)
    )
  }
}
