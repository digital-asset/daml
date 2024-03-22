// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.SimpleExecutionQueue.TaskCell
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.TryUtil.*

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** Functions executed with this class will only run when all previous calls have completed executing.
  * This can be used when async code should not be run concurrently.
  *
  * The default semantics is that a task is only executed if the previous tasks have completed successfully, i.e.,
  * they did not fail nor was the task aborted due to shutdown.
  *
  * If the queue is shutdown, the tasks' execution is aborted due to shutdown too.
  */
class SimpleExecutionQueue(
    name: String,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    logTaskTiming: Boolean = false,
) extends PrettyPrinting
    with NamedLogging
    with FlagCloseableAsync {

  protected val directExecutionContext: DirectExecutionContext =
    DirectExecutionContext(noTracingLogger)

  /** Will execute the given function after all previous executions have completed successfully and return the
    * future with the result of this execution.
    */
  def execute[A](execution: => Future[A], description: String)(implicit
      loggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[A] =
    genExecute(
      runIfFailed = false,
      FutureUnlessShutdown.outcomeF(execution)(directExecutionContext),
      description,
    )

  def executeE[A, B](
      execution: => EitherT[Future, A, B],
      description: String,
  )(implicit loggingContext: ErrorLoggingContext): EitherT[FutureUnlessShutdown, A, B] =
    EitherT(execute(execution.value, description))

  def executeEUS[A, B](
      execution: => EitherT[FutureUnlessShutdown, A, B],
      description: String,
  )(implicit loggingContext: ErrorLoggingContext): EitherT[FutureUnlessShutdown, A, B] =
    EitherT(executeUS(execution.value, description))

  def executeUS[A](
      execution: => FutureUnlessShutdown[A],
      description: String,
      runWhenUnderFailures: => Unit = (),
  )(implicit
      loggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[A] =
    genExecute(runIfFailed = false, execution, description, runWhenUnderFailures)

  def executeUnderFailuresUS[A](execution: => FutureUnlessShutdown[A], description: String)(implicit
      loggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[A] =
    genExecute(runIfFailed = true, execution, description)

  private def genExecute[A](
      runIfFailed: Boolean,
      execution: => FutureUnlessShutdown[A],
      description: String,
      runWhenUnderFailures: => Unit = (),
  )(implicit loggingContext: ErrorLoggingContext): FutureUnlessShutdown[A] = {
    val next = new TaskCell(description, logTaskTiming, futureSupervisor, directExecutionContext)
    val oldHead = queueHead.getAndSet(next) // linearization point
    next.chain(
      oldHead,
      runIfFailed,
      // Only run the task when the queue is not shut down
      performUnlessClosingUSF(s"queued task: $description")(execution)(
        directExecutionContext,
        loggingContext.traceContext,
      ),
      runWhenUnderFailures,
    )
  }

  /** Returns a future that completes when all scheduled tasks up to now have completed or after a shutdown has been initiated. Never fails. */
  def flush(): Future[Unit] =
    queueHead
      .get()
      .future
      .onShutdown(())(directExecutionContext)
      .recover { exception =>
        logger.debug(s"Flush has failed, however returning success", exception)(
          TraceContext.empty
        )
      }(directExecutionContext)

  private val queueHead: AtomicReference[TaskCell] =
    new AtomicReference[TaskCell](TaskCell.sentinel(directExecutionContext))

  /** slow and in-efficient queue size, to be used for inspection */
  def queueSize: Int = {
    @tailrec
    def go(cell: TaskCell, count: Int): Int = cell.predecessor match {
      case None => count
      case Some(predCell) => go(predCell, count + 1)
    }
    go(queueHead.get(), 0)
  }

  /** Returns a sequence of tasks' descriptions in this execution queue.
    * The first entry refers to the last known completed task,
    * the others are running or queued.
    */
  def queued: Seq[String] = {
    @tailrec
    def go(cell: TaskCell, descriptions: List[String]): List[String] = {
      cell.predecessor match {
        case None => s"${cell.description} (completed)" :: descriptions
        case Some(predCell) => go(predCell, cell.description :: descriptions)
      }
    }
    go(queueHead.get(), List.empty[String])
  }

  override def pretty: Pretty[SimpleExecutionQueue] = prettyOfClass(
    param("queued tasks", _.queued.map(_.unquoted))
  )

  private def forceShutdownTasks(): Unit = {
    @tailrec
    def go(cell: TaskCell, nextTaskAfterRunningOne: Option[TaskCell]): Option[TaskCell] = {
      // If the predecessor of the cell is completed, then it is the running task, in which case we stop the recursion.
      // Indeed the predecessor of the running task is only set to None when the task has completed, so we need to
      // access the predecessor and check if it's done. There is a potential race because by the time we reach the supposed
      // first task after the running one and shut it down, it might already have started if the running task finished in the meantime.
      // This is fine though because tasks are wrapped in a performUnlessShutdown so the task will be `AbortedDueToShutdown` anyway
      // instead of actually start, so the race is benign.
      if (cell.predecessor.exists(_.future.unwrap.isCompleted)) {
        errorLoggingContext(TraceContext.empty).debug(
          s"${cell.description} is still running. It will be left running but all subsequent tasks will be aborted."
        )
        nextTaskAfterRunningOne
      } else {
        cell.predecessor match {
          case Some(predCell) => go(predCell, Some(cell))
          case _ => None
        }
      }
    }

    // Find the first task queued after the currently running one and shut it down, this will trigger a cascade and
    // `AbortDueToShutdown` all subsequent tasks
    go(queueHead.get(), None).foreach(_.shutdown())
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    Seq(
      AsyncCloseable(
        s"simple-exec-queue: $name",
        flush(),
        timeouts.shutdownProcessing,
        // In the event where the flush does not complete within the allocated timeout,
        // forcibly shutdown the remaining queued tasks, except the currently running one
        onTimeout = _ => forceShutdownTasks(),
      )
    )
  }
}

object SimpleExecutionQueue {

  /** Implements the chaining of tasks and their descriptions. */
  private class TaskCell(
      val description: String,
      logTaskTiming: Boolean,
      futureSupervisor: FutureSupervisor,
      directExecutionContext: DirectExecutionContext,
  )(implicit errorLoggingContext: ErrorLoggingContext) {

    /** Completes after all earlier tasks and this task have completed.
      * Fails with the exception of the first task that failed, if any.
      */
    private val completionPromise: PromiseUnlessShutdown[Unit] =
      new PromiseUnlessShutdown[Unit](description, futureSupervisor)(
        errorLoggingContext,
        directExecutionContext,
      )

    /** `null` if no predecessor has been chained.
      * [[scala.Some$]]`(cell)` if the predecessor task is `cell` and this task is queued or running.
      * [[scala.None$]] if this task has been completed.
      */
    private val predecessorCell: AtomicReference[Option[TaskCell]] =
      new AtomicReference[Option[TaskCell]]()

    private val taskCreationTime: Long = if (logTaskTiming) System.nanoTime() else 0L

    /** Chains this task cell after its predecessor `pred`. */
    /* The linearization point in the caller `genExecute` has already determined the sequencing of tasks
     * if they are enqueued concurrently. So it now suffices to make sure that this task's future executes after
     * `pred` (unless the previous task's future failed and `runIfFailed` is false) and that
     * we cut the chain to the predecessor thereafter.
     */
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    def chain[A](
        pred: TaskCell,
        runIfFailed: Boolean,
        execution: => FutureUnlessShutdown[A],
        runWhenUnderFailures: => Unit,
    )(implicit
        loggingContext: ErrorLoggingContext
    ): FutureUnlessShutdown[A] = {
      val succeed = predecessorCell.compareAndSet(null, Some(pred))
      ErrorUtil.requireState(succeed, s"Attempt to chain task $description several times.")(
        loggingContext
      )

      def runTask(
          propagatedException: Option[Throwable]
      ): FutureUnlessShutdown[(Option[Throwable], A)] = {
        if (logTaskTiming && loggingContext.logger.underlying.isDebugEnabled) {
          val startTime = System.nanoTime()
          val waitingDelay = Duration.fromNanos(startTime - taskCreationTime)
          loggingContext.debug(
            show"Running task ${description.singleQuoted} after waiting for $waitingDelay"
          )
          execution.transform { result =>
            val finishTime = System.nanoTime()
            val runningDuration = Duration.fromNanos(finishTime - startTime)
            val resultStr = result match {
              case Failure(_exception) => "failed"
              case Success(UnlessShutdown.Outcome(_result)) => "completed"
              case Success(UnlessShutdown.AbortedDueToShutdown) => "aborted"
            }
            loggingContext.debug(
              show"Task ${description.singleQuoted} finished as $resultStr after $runningDuration running time and $waitingDelay waiting time"
            )
            result.map(r => r.map(a => (propagatedException, a)))
          }(directExecutionContext)
        } else {
          execution.map(a => (propagatedException, a))(directExecutionContext)
        }
      }

      val chained = pred.future.transformWith {
        case Success(UnlessShutdown.Outcome(_result)) =>
          runTask(None)
        case Success(UnlessShutdown.AbortedDueToShutdown) =>
          FutureUnlessShutdown.abortedDueToShutdown
        case Failure(ex) =>
          // Propagate the exception `ex` from an earlier task
          if (runIfFailed) runTask(Some(ex))
          else {
            if (logTaskTiming && loggingContext.logger.underlying.isDebugEnabled) {
              val startTime = System.nanoTime()
              val waitingDelay = Duration.fromNanos(startTime - taskCreationTime)
              loggingContext.logger.debug(
                s"Not running task ${description.singleQuoted} due to exception after waiting for $waitingDelay"
              )(loggingContext.traceContext)
            }
            Try(runWhenUnderFailures).forFailed(e =>
              loggingContext.logger.debug(
                s"Failed to run 'runWhenUnderFailures' function for ${description.singleQuoted}",
                e,
              )(loggingContext.traceContext)
            )
            FutureUnlessShutdown.failed(ex)
          }
      }(directExecutionContext)
      val completed = chained.thereafter { _ =>
        // Cut the predecessor as we're now done.
        predecessorCell.set(None)
      }(directExecutionContext)
      val propagatedException = completed.flatMap { case (earlierExceptionO, _) =>
        earlierExceptionO.fold(FutureUnlessShutdown.unit)(FutureUnlessShutdown.failed)
      }(directExecutionContext)
      completionPromise.completeWith(propagatedException)

      // In order to be able to manually shutdown a task using its completionPromise, we semantically "check" that
      // completionPromise hasn't already be completed with AbortedDueToShutdown, and if not we return the computation
      // result. Note that we need to be sure that completionPromise will be fulfilled when
      // 'completed' is, which is done just above
      completionPromise.futureUS.transformWith {
        case Success(AbortedDueToShutdown) => FutureUnlessShutdown.abortedDueToShutdown
        case _ => completed.map(_._2)(directExecutionContext)
      }(directExecutionContext)
    }

    /** The returned future completes after this task has completed or a shutdown has occurred.
      * If the task is not supposed to run if an earlier task has failed or was shutdown,
      * then this task completes when all earlier tasks have completed without being actually run.
      */
    def future: FutureUnlessShutdown[Unit] = completionPromise.futureUS

    /** Returns the predecessor task's cell or [[scala.None$]] if this task has already been completed. */
    def predecessor: Option[TaskCell] = {
      // Wait until the predecessor cell has been set.
      @SuppressWarnings(Array("org.wartremover.warts.Null"))
      @tailrec def go(): Option[TaskCell] = {
        val pred = predecessorCell.get()
        if (pred eq null) go() else pred
      }
      go()
    }

    def shutdown(): Unit = {
      errorLoggingContext.warn(s"Forcibly completing $description with AbortedDueToShutdown")
      completionPromise.shutdown()
    }
  }

  private object TaskCell {

    /** Sentinel task cell that is already completed. */
    def sentinel(directExecutionContext: DirectExecutionContext): TaskCell = {
      // We don't care about the logging context here because the promise is already completed
      import TraceContext.Implicits.Empty.*
      val errorLoggingContext = ErrorLoggingContext.fromTracedLogger(NamedLogging.noopLogger)
      val cell = new TaskCell("sentinel", false, FutureSupervisor.Noop, directExecutionContext)(
        errorLoggingContext
      )
      cell.predecessorCell.set(None)
      cell.completionPromise.outcome(())
      cell
    }
  }
}
