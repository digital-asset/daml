// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.error.FatalError
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.SimpleExecutionQueue.TaskCell
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Determines how the queue reacts to failures of previous tasks.
  */
sealed trait FailureMode

/** Causes the queue to crash the entire process if a task is scheduled after a previously failed task.
  */
object CrashAfterFailure extends FailureMode

/** Causes the queue to not process any further tasks after a previously failed task.
  */
object StopAfterFailure extends FailureMode

/** The queue will continue the execution of tasks even if previous tasks had failed.
  */
object ContinueAfterFailure extends FailureMode

/** Functions executed with this class will only run when all previous calls have completed executing.
  * This can be used when async code should not be run concurrently.
  *
  * The default semantics is that a task is only executed if the previous tasks have completed successfully, i.e.,
  * they did not fail nor was the task aborted due to shutdown.
  *
  * If the queue is shutdown, the tasks' execution is aborted due to shutdown too.
  *
  * @param name For logging purposes
  * @param logTaskTiming If true logs wait and run time for each of the tasks
  * @param failureMode How the queue handles the execution of tasks after a previous task had failed
  */
class SimpleExecutionQueue(
    private val name: String,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    private val logTaskTiming: Boolean,
    failureMode: FailureMode,
) extends PrettyPrinting
    with NamedLogging
    with FlagCloseableAsync {

  /** @param name For logging purposes
    * @param logTaskTiming If true, logs wait and run time for each of the tasks
    * @param crashOnFailure If true, crash when a task fails (because the queue is then stuck)
    */
  def this(
      name: String,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      logTaskTiming: Boolean = false,
      crashOnFailure: Boolean,
  ) = this(
    name,
    futureSupervisor,
    timeouts,
    loggerFactory,
    logTaskTiming,
    if (crashOnFailure) CrashAfterFailure else StopAfterFailure,
  )

  protected val directExecutionContext: DirectExecutionContext =
    DirectExecutionContext(noTracingLogger)

  /** Will execute the given function after all previous executions have completed successfully and return the
    * future with the result of this execution.
    */
  def execute[A](execution: => Future[A], description: String)(implicit
      loggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[A] =
    genExecute(
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
  )(implicit
      loggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[A] = genExecute(execution, description)

  private def genExecute[A](
      execution: => FutureUnlessShutdown[A],
      description: String,
  )(implicit loggingContext: ErrorLoggingContext): FutureUnlessShutdown[A] = {
    val next =
      new TaskCell(
        queueName = name,
        description = description,
        logTaskTiming,
        failureMode,
        futureSupervisor,
        directExecutionContext,
      )
    val oldHead = queueHead.getAndSet(next) // linearization point
    next.chain(
      oldHead,
      // Only run the task when the queue is not shut down
      performUnlessClosingF(s"queued task: $description")(
        // Turn the action FutureUnlessShutdown[A] into Future[Try[UnlessShutdown[A]]].
        // This allows us to distinguish between the failure/shutdown of the queue or the task.
        execution
          .transformIntoSuccess(UnlessShutdown.Outcome(_))(directExecutionContext)
          .onShutdown(Success(UnlessShutdown.AbortedDueToShutdown))(directExecutionContext)
      )(
        directExecutionContext,
        loggingContext.traceContext,
      ),
    )
  }

  /** Returns a future that completes when all scheduled tasks up to now have completed or after a shutdown has been initiated. Never fails. */
  def flush(): Future[Unit] =
    queueHead
      .get()
      .future
      .map(_ => ())(directExecutionContext)
      .onShutdown(())(directExecutionContext)
      .recover { exception =>
        logger.debug(s"Flush has failed, however returning success", exception)(
          TraceContext.empty
        )
      }(directExecutionContext)

  private val queueHead: AtomicReference[TaskCell] =
    new AtomicReference[TaskCell](TaskCell.sentinel(queueName = name, directExecutionContext))

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
    def go(cell: TaskCell, descriptions: List[String]): List[String] =
      cell.predecessor match {
        case None => s"${cell.description} (completed)" :: descriptions
        case Some(predCell) => go(predCell, cell.description :: descriptions)
      }
    go(queueHead.get(), List.empty[String])
  }

  override protected def pretty: Pretty[SimpleExecutionQueue] = prettyOfClass(
    param("queued tasks", _.queued.map(_.unquoted))
  )

  private def forceShutdownTasks(): Unit = {
    @tailrec
    def go(cell: TaskCell, nextTaskAfterRunningOne: Option[TaskCell]): Option[TaskCell] =
      // If the predecessor of the cell is completed, then cell is the running task, in which case we stop the recursion.
      // Indeed the predecessor of the running task is only set to None when the task has completed, so we need to
      // access the predecessor and check if it's done. There is a potential race because by the time we reach the supposed
      // first task after the running one and shut it down, it might already have started if the running task finished in the meantime.
      // This is fine though because tasks are wrapped in a performUnlessShutdown so the task will be `AbortedDueToShutdown` anyway
      // instead of actually start, so the race is benign.
      cell.predecessor match {
        case Some(predCell) if predCell.future.unwrap.isCompleted =>
          errorLoggingContext(TraceContext.empty).debug(
            s"${cell.description} is still running. It will be left running but all subsequent tasks will be aborted."
          )
          nextTaskAfterRunningOne
        case None =>
          errorLoggingContext(TraceContext.empty).debug(
            s"${cell.description} is about to complete. All subsequent tasks will be aborted."
          )
          nextTaskAfterRunningOne
        case Some(predCell) => go(predCell, Some(cell))
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
      val queueName: String,
      val description: String,
      logTaskTiming: Boolean,
      failureMode: FailureMode,
      futureSupervisor: FutureSupervisor,
      directExecutionContext: DirectExecutionContext,
  )(implicit errorLoggingContext: ErrorLoggingContext) {

    /** Completes after all earlier tasks and this task have completed.
      * The result of the executed action will be captured by `Try[UnlessShutdown[Unit]]`.
      * The promise failure/shutdown of the promise itself only reflects the queue's failure/shutdown status.
      */
    private val completionPromise: PromiseUnlessShutdown[Try[UnlessShutdown[Unit]]] =
      PromiseUnlessShutdown.supervised[Try[UnlessShutdown[Unit]]](description, futureSupervisor)(
        errorLoggingContext
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
     * `pred` (unless the previous task's future failed and `failureMode` is not ContinueAfterFailure) and that
     * we cut the chain to the predecessor thereafter.
     * Of the type `FutureUnlessShutdown[Try[UnlessShutdown[A]]]`, FutureUnlessShutdown reflects the queue's status,
     * and `Try[UnlessShutdown[A]]` is the outcome of `execution`.
     */
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    def chain[A](
        pred: TaskCell,
        execution: => FutureUnlessShutdown[Try[UnlessShutdown[A]]],
    )(implicit
        loggingContext: ErrorLoggingContext
    ): FutureUnlessShutdown[A] = {
      val succeed = predecessorCell.compareAndSet(null, Some(pred))
      ErrorUtil.requireState(succeed, s"Attempt to chain task $description several times.")(
        loggingContext
      )

      def runTask(): FutureUnlessShutdown[Try[UnlessShutdown[A]]] =
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
              // failure of the queue itself
              case Failure(_exception) => "queue-failed"
              // shutdown of the queue itself
              case Success(UnlessShutdown.AbortedDueToShutdown) => "queue-shutdown"

              // task execution was successful
              case Success(UnlessShutdown.Outcome(Success(UnlessShutdown.Outcome(_result)))) =>
                "completed"
              // task execution has been shut down
              case Success(UnlessShutdown.Outcome(Success(AbortedDueToShutdown))) => "aborted"
              // task execution failed
              case Success(UnlessShutdown.Outcome(Failure(_exception))) => "failed"
            }
            loggingContext.debug(
              show"Task ${description.singleQuoted} finished as $resultStr after $runningDuration running time and $waitingDelay waiting time"
            )
            result
          }(directExecutionContext)
        } else {
          execution
        }

      // CrashAfterFailure only runs the subsequent task if the previous task was successful.
      // In case of the previous task's failure, it will crash the process.
      def proceedWithCrashAfterFailure(
          previousResult: Try[UnlessShutdown[Unit]]
      ): FutureUnlessShutdown[Try[UnlessShutdown[A]]] =
        previousResult match {
          case Failure(ex) =>
            logNotRunningTask(isShutdown = false)
            FatalError.exitOnFatalError(
              message =
                s"Execution queue $queueName is stuck, task ${description.singleQuoted} will not run because of failure of previous task",
              exception = ex,
              logger = loggingContext.logger,
            )(loggingContext.traceContext)

          case Success(AbortedDueToShutdown) =>
            logNotRunningTask(isShutdown = true)
            // Do not crash if a previous task was aborted due to shutdown,
            // because we might be in the middle of a coordinated shutdown and therefore
            // we don't want to just sys.exit.
            FutureUnlessShutdown.pure(Success(UnlessShutdown.AbortedDueToShutdown))

          case Success(UnlessShutdown.Outcome(_)) =>
            runTask()
        }

      // StopAfterFailure only runs the subsequent task if the previous task was successful
      def proceedWithStopAfterFailure(
          previousResult: Try[UnlessShutdown[Unit]]
      ): FutureUnlessShutdown[Try[UnlessShutdown[A]]] =
        previousResult match {
          case Failure(exception) =>
            logNotRunningTask(isShutdown = false)
            FutureUnlessShutdown.pure(Failure(exception))

          case Success(UnlessShutdown.AbortedDueToShutdown) =>
            logNotRunningTask(isShutdown = true)
            FutureUnlessShutdown.pure(Success(UnlessShutdown.AbortedDueToShutdown))

          case Success(UnlessShutdown.Outcome(_)) =>
            runTask()
        }

      def logNotRunningTask(isShutdown: Boolean): Unit = {
        def log(msg: String): Unit =
          if (isShutdown) loggingContext.logger.debug(msg)(loggingContext.traceContext)
          else loggingContext.logger.error(msg)(loggingContext.traceContext)
        val reason = if (isShutdown) "shutdown" else "failure"
        val primaryMessage =
          s"Task ${description.singleQuoted} will not run because of $reason of previous task"

        if (logTaskTiming) {
          val startTime = System.nanoTime()
          val waitingDelay = Duration.fromNanos(startTime - taskCreationTime)
          log(s"$primaryMessage after waiting for $waitingDelay")
        } else {
          log(primaryMessage)
        }
      }

      val chained = pred.future.transformWith {
        case Failure(exception) =>
          logNotRunningTask(isShutdown = false)
          FutureUnlessShutdown.failed(exception)

        case Success(UnlessShutdown.Outcome(executionResult)) =>
          failureMode match {
            case CrashAfterFailure =>
              proceedWithCrashAfterFailure(executionResult)

            case StopAfterFailure =>
              proceedWithStopAfterFailure(executionResult)

            case ContinueAfterFailure =>
              // this failureMode always runs the next task, independent of the previous task's result
              runTask()
          }
        case Success(AbortedDueToShutdown) =>
          logNotRunningTask(isShutdown = true)
          FutureUnlessShutdown.abortedDueToShutdown
      }(directExecutionContext)

      val completed = {
        implicit val ec: ExecutionContext = directExecutionContext
        // Cut the predecessor as we're now done.
        chained.thereafter(_ => predecessorCell.set(None))
      }
      completionPromise
        .completeWithUS(
          completed.map(_.map(_.map(_ => ())))(directExecutionContext)
        )
        .discard

      // In order to be able to manually shutdown a task using its completionPromise, we semantically "check" that
      // completionPromise hasn't already be completed with AbortedDueToShutdown, and if not we return the computation
      // result. Note that we need to be sure that completionPromise will be fulfilled when
      // 'completed' is, which is done just above
      completionPromise.futureUS.transformWith {
        case Success(AbortedDueToShutdown) => FutureUnlessShutdown.abortedDueToShutdown
        case _ =>
          completed
            .flatMap(tryUS => FutureUnlessShutdown(Future.fromTry(tryUS)))(directExecutionContext)
      }(directExecutionContext)
    }

    /** The returned future completes after this task has completed or a shutdown has occurred.
      * If the task is not supposed to run if an earlier task has failed or was shutdown,
      * then this task completes when all earlier tasks have completed without being actually run.
      */
    def future: FutureUnlessShutdown[Try[UnlessShutdown[Unit]]] = completionPromise.futureUS

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
    def sentinel(queueName: String, directExecutionContext: DirectExecutionContext): TaskCell = {
      // We don't care about the logging context here because the promise is already completed
      import TraceContext.Implicits.Empty.*
      val errorLoggingContext = ErrorLoggingContext.fromTracedLogger(NamedLogging.noopLogger)
      val cell = new TaskCell(
        queueName = queueName,
        description = "sentinel",
        logTaskTiming = false,
        failureMode = StopAfterFailure,
        FutureSupervisor.Noop,
        directExecutionContext,
      )(
        errorLoggingContext
      )
      cell.predecessorCell.set(None)
      cell.completionPromise.outcome(Success(UnlessShutdown.Outcome(())))
      cell
    }
  }
}
