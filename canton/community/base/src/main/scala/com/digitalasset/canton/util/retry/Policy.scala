// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import cats.Eval
import cats.syntax.flatMap.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PerformUnlessClosing,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.retry.RetryUtil.{
  AllExnRetryable,
  ErrorKind,
  ExceptionRetryable,
  NoErrorKind,
}
import com.digitalasset.canton.util.retry.RetryWithDelay.{RetryOutcome, RetryTermination}
import com.digitalasset.canton.util.{DelayUtil, LoggerUtil}
import org.slf4j.event.Level

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

/** A retry [[com.digitalasset.canton.util.retry.Policy]] defines an interface for retrying a future-based task with
  * retry semantics specific to implementations. If the task throws a non-fatal exceptions synchronously, the exception is
  * converted into an asynchronous one, i.e., it is returned as a failed future or retried.
  *
  * If unsure about what retry policy to pick, [[com.digitalasset.canton.util.retry.Backoff]] is a good default.
  */
abstract class Policy(logger: TracedLogger) {

  protected val directExecutionContext: DirectExecutionContext = DirectExecutionContext(logger)

  def apply[T](task: => Future[T], retryOk: ExceptionRetryable)(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[T]

  def unlessShutdown[T](task: => FutureUnlessShutdown[T], retryOk: ExceptionRetryable)(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[T]

}

object Policy {

  /** Repeatedly execute the task until it doesn't throw an exception or the `flagCloseable` is closing. */
  def noisyInfiniteRetry[A](
      task: => Future[A],
      performUnlessClosing: PerformUnlessClosing,
      retryInterval: FiniteDuration,
      operationName: String,
      actionable: String,
  )(implicit
      loggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[A] =
    noisyInfiniteRetryUS(
      FutureUnlessShutdown.outcomeF(task),
      performUnlessClosing,
      retryInterval,
      operationName,
      actionable,
    )

  /** Repeatedly execute the task until it returns an abort due to shutdown, doesn't throw an exception, or the `flagCloseable` is closing. */
  def noisyInfiniteRetryUS[A](
      task: => FutureUnlessShutdown[A],
      performUnlessClosing: PerformUnlessClosing,
      retryInterval: FiniteDuration,
      operationName: String,
      actionable: String,
  )(implicit
      loggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[A] =
    Pause(
      loggingContext.logger,
      performUnlessClosing,
      maxRetries = Int.MaxValue,
      retryInterval,
      operationName = operationName,
      actionable = Some(actionable),
    ).unlessShutdown(task, AllExnRetryable)(
      Success.always,
      executionContext,
      loggingContext.traceContext,
    )
}

abstract class RetryWithDelay(
    logger: TracedLogger,
    operationName: String,
    longDescription: String,
    actionable: Option[String], // How to mitigate the error
    initialDelay: FiniteDuration,
    totalMaxRetries: Int,
    performUnlessClosing: PerformUnlessClosing,
    retryLogLevel: Option[Level],
    suspendRetries: Eval[FiniteDuration],
) extends Policy(logger) {

  private val complainAfterRetries: Int = 10

  private val actionableMessage: String = actionable.map(" " + _).getOrElse("")

  protected def nextDelay(nextCount: Int, delay: FiniteDuration): FiniteDuration

  /** A [[com.digitalasset.canton.util.retry.Success]] criteria is supplied
    * to determine whether the future-based task has succeeded, or if it should perhaps be retried. Retries are not
    * performed after the [[com.digitalasset.canton.lifecycle.FlagCloseable]] has been closed. In that case, the
    * Future is completed with the last result (even if it is an outcome that doesn't satisfy the `success` predicate).
    */
  override def apply[T](
      task: => Future[T],
      retryable: ExceptionRetryable,
  )(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[T] =
    retryWithDelay(task, retryable, executionContext).transform {
      case util.Success(RetryOutcome(outcome, _termination)) =>
        outcome
      case Failure(failure) =>
        logger.error("retryWithDelay failed unexpectedly", failure)
        Failure(failure)
    }(directExecutionContext)

  /** In contrast to [[com.digitalasset.canton.util.retry.RetryWithDelay.apply]], this Policy completes the returned
    * future with `AbortedDueToShutdown` if the retry is aborted due to the corresponding
    * [[com.digitalasset.canton.lifecycle.FlagCloseable]] being closed or if the task itself reports a shutdown (and
    * not with the last result).
    *
    * Unless your task does already naturally return a `FutureUnlessShutdown[T]`, using
    * [[com.digitalasset.canton.util.retry.RetryWithDelay.apply]] is likely sufficient to make it robust against
    * shutdowns.
    */
  override def unlessShutdown[T](
      task: => FutureUnlessShutdown[T],
      retryable: ExceptionRetryable,
  )(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[T] =
    FutureUnlessShutdown {
      retryWithDelay(task.unwrap, retryable, executionContext)(Success.onShutdown, implicitly)
        .transform {
          case util.Success(outcome) =>
            util.Success(outcome.toUnlessShutdown.flatten)
          case Failure(failure) =>
            logger.error("retryWithDelay failed unexpectedly", failure)
            Failure(failure)
        }(directExecutionContext)
    }

  private def retryWithDelay[T](
      task: => Future[T],
      retryable: ExceptionRetryable,
      executionContext: ExecutionContext,
  )(implicit success: Success[T], traceContext: TraceContext): Future[RetryOutcome[T]] = {
    implicit val loggingContext: ErrorLoggingContext = ErrorLoggingContext.fromTracedLogger(logger)

    import LoggerUtil.logOnThrow

    def runTask(): Future[T] = Future.fromTry(Try(task)).flatten

    def run(
        previousResult: Future[T],
        totalRetries: Int,
        lastErrorKind: ErrorKind,
        retriesOfLastErrorKind: Int,
        delay: FiniteDuration,
    ): Future[RetryOutcome[T]] = logOnThrow {
      previousResult.transformWith { x =>
        logOnThrow(
          x match {
            case succ @ util.Success(result) if success.predicate(result) =>
              logger.trace(
                s"The operation '$operationName' was successful. No need to retry. $longDescription"
              )
              Future.successful(RetryOutcome(succ, RetryTermination.Success))

            case outcome if performUnlessClosing.isClosing =>
              logger.debug(
                s"Giving up on retrying the operation '$operationName' due to shutdown. Last attempt was $lastErrorKind"
              )
              Future.successful(RetryOutcome(outcome, RetryTermination.Shutdown))

            case outcome if totalMaxRetries < Int.MaxValue && totalRetries >= totalMaxRetries =>
              logger.info(
                messageOfOutcome(
                  outcome,
                  s"Total maximum number of retries $totalMaxRetries exceeded. Giving up.",
                ),
                throwableOfOutcome(outcome),
              )
              Future.successful(RetryOutcome(outcome, RetryTermination.GiveUp))

            case outcome =>
              // this will also log the exception in outcome
              val errorKind = retryable.retryOK(outcome, logger, Some(lastErrorKind))
              val retriesOfErrorKind = if (errorKind == lastErrorKind) retriesOfLastErrorKind else 0
              if (
                errorKind.maxRetries == Int.MaxValue || retriesOfErrorKind < errorKind.maxRetries
              ) {
                val suspendDuration = suspendRetries.value
                if (suspendDuration > Duration.Zero) {
                  logger.info(
                    s"Suspend retrying the operation '$operationName' for $suspendDuration."
                  )
                  DelayUtil
                    .delayIfNotClosing(operationName, suspendDuration, performUnlessClosing)
                    .onShutdown(())(directExecutionContext)
                    .flatMap(_ => run(previousResult, 0, errorKind, 0, initialDelay))(
                      directExecutionContext
                    )
                } else {
                  val level = retryLogLevel.getOrElse {
                    if (totalRetries < complainAfterRetries || totalMaxRetries != Int.MaxValue)
                      Level.INFO
                    else Level.WARN
                  }
                  val change = if (errorKind == lastErrorKind) {
                    ""
                  } else {
                    s"New kind of error: $errorKind. "
                  }
                  LoggerUtil.logAtLevel(
                    level,
                    messageOfOutcome(outcome, show"${change}Retrying after $delay."),
                    // No need to log the exception in the outcome, as this has been logged by retryable.retryOk.
                  )

                  val delayedF =
                    DelayUtil.delayIfNotClosing(operationName, delay, performUnlessClosing)
                  delayedF
                    .flatMap { _ =>
                      logOnThrow {
                        LoggerUtil.logAtLevel(
                          level,
                          s"Now retrying operation '$operationName'. $longDescription$actionableMessage",
                        )
                        // Run the task again on the normal execution context as the task might take a long time.
                        // `performUnlessClosingF` guards against closing the execution context.
                        val nextRunUnlessShutdown =
                          performUnlessClosing.performUnlessClosingF(operationName)(runTask())(
                            executionContext,
                            traceContext,
                          )
                        @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
                        val nextRunF = nextRunUnlessShutdown
                          .onShutdown {
                            // If we're closing, report the previous `outcome` and recurse.
                            // This will enter the case branch with `flagCloseable.isClosing`
                            // and therefore yield the termination reason `Shutdown`.
                            outcome.get
                          }(
                            // Use the direct execution context as this is a small task.
                            // The surrounding `performUnlessClosing` ensures that this post-processing
                            // is registered with the normal execution context before it can close.
                            directExecutionContext
                          )
                        FutureUnlessShutdown.outcomeF(
                          run(
                            nextRunF,
                            totalRetries + 1,
                            errorKind,
                            retriesOfErrorKind + 1,
                            nextDelay(totalRetries + 1, delay),
                          )
                        )(executionContext)
                      }
                    }(
                      // It is safe to use the general execution context here by the following argument.
                      // - If the `onComplete` executes before `DelayUtil` completes the returned promise,
                      //   then the completion of the promise will schedule the function immediately.
                      //   Since this completion is guarded by `performUnlessClosing`,
                      //   the body gets scheduled with `executionContext` before `flagCloseable`'s close method completes.
                      // - If `DelayUtil` completes the returned promise before the `onComplete` call executes,
                      //   the `onComplete` call itself will schedule the body
                      //   and this is guarded by the `performUnlessClosing` above.
                      // Therefore the execution context is still open when the scheduling happens.
                      executionContext
                    )
                    .onShutdown(
                      RetryOutcome(outcome, RetryTermination.Shutdown)
                    )(executionContext)
                }
              } else {
                logger.info(
                  messageOfOutcome(
                    outcome,
                    s"Maximum number of retries ${errorKind.maxRetries} exceeded. Giving up.",
                    // No need to log the exception in outcome, as this has been logged by retryable.retryOk.
                  )
                )
                Future.successful(RetryOutcome(outcome, RetryTermination.GiveUp))
              }
          }
        )
      // Although in most cases, it may be ok to schedule the body on executionContext,
      // there is a chance that executionContext is closed when the body is scheduled.
      // By choosing directExecutionContext, we avoid a RejectedExecutionException in this case.
      }(directExecutionContext)
    }

    // Run 0: Run task without checking `flagCloseable`. If necessary, the client has to check for closing.
    // Run 1 onwards: Only run this if `flagCloseable` is not closing.
    //  (The check is performed at the recursive call.)
    //  Checking at the client would be very difficult, because the client would have to deal with a closed EC.
    run(runTask(), 0, NoErrorKind, 0, initialDelay)
  }

  private def messageOfOutcome(
      outcome: Try[Any],
      consequence: String,
  ): String = outcome match {
    case util.Success(result) =>
      s"The operation '$operationName' was not successful. $consequence Result: $result. $longDescription"
    case Failure(_) =>
      s"The operation '$operationName' has failed with an exception. $consequence $longDescription$actionableMessage"
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def throwableOfOutcome(outcome: Try[Any]): Throwable = outcome.fold(identity, null)
}

object RetryWithDelay {

  /** The outcome of the last run of the task,
    * along with the condition that stopped the retry.
    */
  private final case class RetryOutcome[A](outcome: Try[A], termination: RetryTermination) {

    /** @throws java.lang.Throwable Rethrows the exception if [[outcome]] is a [[scala.util.Failure]] */
    @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
    def toUnlessShutdown: UnlessShutdown[A] = {
      if (termination == RetryTermination.Shutdown) AbortedDueToShutdown
      else Outcome(outcome.get)
    }
  }
  private sealed trait RetryTermination extends Product with Serializable
  private[RetryWithDelay] object RetryTermination {

    /** The task completed successfully */
    case object Success extends RetryTermination

    /** The retry limit was exceeded or an exception was deemed not retryable */
    case object GiveUp extends RetryTermination

    /** The retry stopped due to shutdown */
    case object Shutdown extends RetryTermination
  }
}

/** Retry immediately after failure for a max number of times */
final case class Directly(
    logger: TracedLogger,
    performUnlessClosing: PerformUnlessClosing,
    maxRetries: Int,
    operationName: String,
    longDescription: String = "",
    retryLogLevel: Option[Level] = None,
    suspendRetries: Eval[FiniteDuration] = Eval.now(Duration.Zero),
) extends RetryWithDelay(
      logger,
      operationName,
      longDescription,
      None,
      Duration.Zero,
      maxRetries,
      performUnlessClosing,
      retryLogLevel,
      suspendRetries,
    ) {

  override def nextDelay(nextCount: Int, delay: FiniteDuration): FiniteDuration = Duration.Zero
}

/** Retry with a pause between attempts for a max number of times */
final case class Pause(
    logger: TracedLogger,
    performUnlessClosing: PerformUnlessClosing,
    maxRetries: Int,
    delay: FiniteDuration,
    operationName: String,
    longDescription: String = "",
    actionable: Option[String] = None,
    retryLogLevel: Option[Level] = None,
    suspendRetries: Eval[FiniteDuration] = Eval.now(Duration.Zero),
) extends RetryWithDelay(
      logger,
      operationName,
      longDescription,
      actionable,
      delay,
      maxRetries,
      performUnlessClosing,
      retryLogLevel,
      suspendRetries,
    ) {

  override def nextDelay(nextCount: Int, delay: FiniteDuration): FiniteDuration = delay
}

/** A retry policy which will back off using a configurable policy which
  *  incorporates random jitter. This has the advantage of reducing contention
  *  if you have threaded clients using the same service.
  *
  *  {{{
  *  val policy = retry.Backoff()
  *  val future = policy(issueRequest)
  *  }}}
  *
  *  The following pre-made jitter algorithms are available for you to use:
  *
  *  - [[Jitter.none]]
  *  - [[Jitter.full]]
  *  - [[Jitter.equal]]
  *  - [[Jitter.decorrelated]]
  *
  *  You can choose one like this:
  *  {{{
  *  implicit val jitter = retry.Jitter.full(cap = 5.minutes)
  *  val policy = retry.Backoff(1 second)
  *  val future = policy(issueRequest)
  *  }}}
  *
  *  If a jitter policy isn't in scope, it will use [[Jitter.full]] by
  *  default which tends to cause clients slightly less work at the cost of
  *  slightly more time.
  *
  *  For more information about the algorithms, see the following article:
  *
  *  [[https://www.awsarchitectureblog.com/2015/03/backoff.html]]
  *
  *  If the retry is not successful after `maxRetries`, the future is completed with its last result.
  */
final case class Backoff(
    logger: TracedLogger,
    flagCloseable: PerformUnlessClosing,
    maxRetries: Int,
    initialDelay: FiniteDuration,
    maxDelay: Duration,
    operationName: String,
    longDescription: String = "",
    actionable: Option[String] = None,
    retryLogLevel: Option[Level] = None,
    suspendRetries: Eval[FiniteDuration] = Eval.now(Duration.Zero),
)(implicit jitter: Jitter = Jitter.full(maxDelay))
    extends RetryWithDelay(
      logger,
      operationName,
      longDescription,
      actionable,
      initialDelay,
      maxRetries,
      flagCloseable,
      retryLogLevel,
      suspendRetries,
    ) {

  override def nextDelay(nextCount: Int, delay: FiniteDuration): FiniteDuration =
    jitter(initialDelay, delay, nextCount)
}

/** A retry policy in which the failure determines the way a future should be retried.
  *  The partial function `depends` provided may define the domain of both the success OR exceptional
  *  failure of a future fails explicitly.
  *
  *  {{{
  *  val policy = retry.When {
  *    case RetryAfter(retryAt) => retry.Pause(delay = retryAt)
  *  }
  *  val future = policy(issueRequest)
  *  }}}
  *
  *  If the result is not defined for the depends block, the future will not
  *  be retried.
  */
final case class When(
    logger: TracedLogger,
    depends: PartialFunction[Any, Policy],
) extends Policy(logger) {

  override def apply[T](task: => Future[T], retryable: ExceptionRetryable)(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[T] = {
    val fut = Future.fromTry(Try(task)).flatten
    fut
      .flatMap { res =>
        if (success.predicate(res) || !depends.isDefinedAt(res)) fut
        else depends(res)(task, retryable)
      }(directExecutionContext)
      .recoverWith { case NonFatal(e) =>
        if (depends.isDefinedAt(e) && retryable.retryOK(Failure(e), logger, None).maxRetries > 0)
          depends(e)(task, retryable)
        else fut
      }(directExecutionContext)
  }

  override def unlessShutdown[T](task: => FutureUnlessShutdown[T], retryOk: ExceptionRetryable)(
      implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[T] =
    FutureUnlessShutdown(apply(task.unwrap, retryOk)(Success.onShutdown, implicitly, implicitly))
}
