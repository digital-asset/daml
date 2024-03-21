// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import java.util.regex.Pattern
import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, *}
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.math.Ordered.*
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object FutureUtil {

  /** If the future fails, log the associated error and re-throw. The returned future completes after logging.
    */
  def logOnFailure[T](
      future: Future[T],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): Future[T] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(loggingContext.noTracingLogger)
    future.recover {
      // Catching NonFatal only, because a future cannot fail with fatal throwables.
      // Also, it may be a bad idea to run a callback after an OutOfMemoryError.
      case NonFatal(err) =>
        // if the optional close context is closing down, log at most with INFO
        if (closeContext.exists(_.context.isClosing) && level > Level.INFO) {
          LoggerUtil.logThrowableAtLevel(
            Level.INFO,
            s"Logging the following failure on INFO instead of ${level} due to an ongoing shutdown: $failureMessage",
            err,
          )
        } else {
          LoggerUtil.logThrowableAtLevel(level, failureMessage, err)
        }
        try {
          onFailure(err)
        } catch {
          case t: Throwable => // Catching all throwables, because we are merely logging.
            // Always log at ERROR independent of `level` because we don't expect `onFailure` to throw.
            loggingContext.logger.error(
              "An unexpected exception occurred while handling a failed future.",
              t,
            )(loggingContext.traceContext)
            t.addSuppressed(err)
            throw t
        }
        throw err
    }
  }

  /** If the future fails, log the associated error and re-throw. The returned future completes after logging.
    */
  def logOnFailureUnlessShutdown[T](
      future: FutureUnlessShutdown[T],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): FutureUnlessShutdown[T] = {
    FutureUnlessShutdown(
      logOnFailure(future.unwrap, failureMessage, onFailure, level, closeContext)
    )
  }

  /** Discard `future` and log an error if it does not complete successfully.
    * This is useful to document that a `Future` is intentionally not being awaited upon.
    */
  def doNotAwait(
      future: Future[_],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): Unit = {
    val _ = logOnFailure(future, failureMessage, onFailure, level, closeContext)
  }

  /** [[doNotAwait]] but for FUS
    */
  def doNotAwaitUnlessShutdown(
      future: FutureUnlessShutdown[_],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): Unit = {
    doNotAwait(future.unwrap, failureMessage, onFailure, level, closeContext)
  }

  /** Variant of [[doNotAwait]] that also catches non-fatal errors thrown while constructing the future. */
  def catchAndDoNotAwait(
      future: => Future[_],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
  )(implicit loggingContext: ErrorLoggingContext): Unit = {
    val wrappedFuture = Future.fromTry(Try(future)).flatten
    doNotAwait(wrappedFuture, failureMessage, onFailure, level)
  }

  /** Await the completion of `future`. Log a message if the future does not complete within `timeout`.
    * If the `future` fails with an exception within `timeout`, this method rethrows the exception.
    *
    * Instead of using this method, you should use the respective method on one of the ProcessingTimeouts
    *
    * @return Optionally the completed value of `future` if it successfully completes in time.
    */
  @Deprecated
  def valueOrLog[T](
      future: Future[T],
      timeoutMessage: => String,
      timeout: Duration,
      level: Level = Level.WARN,
      stackTraceFilter: Thread => Boolean = defaultStackTraceFilter,
  )(implicit loggingContext: ErrorLoggingContext): Option[T] = {
    // Use Await.ready instead of Await.result to be able to tell the difference between the awaitable throwing a
    // TimeoutException and a TimeoutException being thrown because the awaitable is not ready.
    val ready = Try(Await.ready(future, timeout))
    ready match {
      case Success(awaited) =>
        val result = awaited.value.getOrElse(
          throw new RuntimeException(s"Future $future not completed after successful Await.ready.")
        )
        result.fold(throw _, Some(_))

      case Failure(timeoutExc: TimeoutException) =>
        val stackTraces = StackTraceUtil.formatStackTrace(stackTraceFilter)
        if (stackTraces.isEmpty)
          LoggerUtil.logThrowableAtLevel(level, timeoutMessage, timeoutExc)
        else
          LoggerUtil.logThrowableAtLevel(
            level,
            s"$timeoutMessage\nStack traces:\n$stackTraces",
            timeoutExc,
          )
        None

      case Failure(exc) => ErrorUtil.internalError(exc)
    }
  }

  /** Await the result of a future, logging periodically if the future is taking "too long".
    *
    * This function should generally not be used directly. Rather, use one of the ProcessingTimeout timeouts
    * and wait on that one, ensuring that we do not have stray hard coded timeout values sprinkled through
    * of our code.
    *
    * @param future The future to await
    * @param description A description of the future, for logging
    * @param timeout The timeout for the future to complete within
    * @param warnAfter The amount of time to wait for the future to complete before starting to complain.
    * @param killAwait A kill-switch for the noisy await
    */
  @Deprecated
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def noisyAwaitResult[T](
      future: Future[T],
      description: => String,
      timeout: Duration = Duration.Inf,
      warnAfter: Duration = 1.minute,
      killAwait: Unit => Boolean = _ => false,
      stackTraceFilter: Thread => Boolean = defaultStackTraceFilter,
      onTimeout: TimeoutException => Unit = _ => (),
  )(implicit loggingContext: ErrorLoggingContext): T = {
    val warnAfterAdjusted = {
      // if warnAfter is larger than timeout, make a sensible choice
      if (timeout.isFinite && warnAfter.isFinite && warnAfter > timeout) {
        timeout / 2
      } else warnAfter
    }

    // Use Await.ready instead of Await.result to be able to tell the difference between the awaitable throwing a
    // TimeoutException and a TimeoutException being thrown because the awaitable is not ready.
    def ready(f: Future[T], d: Duration): Try[Future[T]] = Try(Await.ready(f, d))
    def log(level: Level, message: String): Unit = LoggerUtil.logAtLevel(level, message)

    // TODO(i4008) increase the log level to WARN
    val res =
      noisyAwaitResultForTesting(
        future,
        description,
        timeout,
        log,
        () => System.nanoTime(),
        warnAfterAdjusted,
        killAwait,
        stackTraceFilter,
      )(ready)

    res match {
      case Failure(ex: TimeoutException) => onTimeout(ex)
      case _ => ()
    }

    res.get
  }

  lazy val defaultStackTraceFilter: Thread => Boolean = {
    // Include threads directly used by Canton (incl. tests).
    // Excludes threads used by the ledger api server, grpc, ...
    val patterns = Seq(
      ".*-env-execution-context.*",
      ".*-test-execution-context.*",
      ".*-env-scheduler.*",
      ".*-test-execution-context-monitor.*",
      ".*-wallclock.*",
      ".*-remoteclock.*",
      ".*delay-util.*",
      ".*-ccf-execution-context.*",
      ".*-fabric-sequencer-execution-context.*",
      ".*-db-execution-context.*",
      "ScalaTest-run.*",
    )

    // Take the disjunction of patterns.
    val isRelevant = Pattern
      .compile(patterns.map(p => s"($p)").mkString("|"))
      .asMatchPredicate()
    thread => isRelevant.test(thread.getName)
  }

  @VisibleForTesting
  private[util] def noisyAwaitResultForTesting[T](
      future: Future[T],
      description: => String,
      timeout: Duration,
      log: (Level, String) => Unit,
      nanoTime: () => Long,
      warnAfter: Duration,
      killAwait: Unit => Boolean = _ => false,
      stackTraceFilter: Thread => Boolean,
  )(ready: (Future[T], Duration) => Try[Future[T]]): Try[T] = {

    require(warnAfter >= Duration.Zero, show"warnAfter must not be negative: $warnAfter")

    val startTime = nanoTime()

    @tailrec def retry(remaining: Duration, interval: Duration): Try[T] = {

      if (killAwait(())) {
        throw new TimeoutException(s"Noisy await result $description cancelled with kill-switch.")
      }

      val toWait = remaining
        .min(interval)
        // never wait more than 10 seconds to prevent starving on excessively long awaits
        .min(10.seconds)

      if (toWait > Duration.Zero) {
        ready(future, toWait) match {
          case Success(future) =>
            future.value.getOrElse(
              Failure(
                new RuntimeException(
                  s"Future $future not complete after successful Await.ready, this should never happen"
                )
              )
            )

          case Failure(ex: TimeoutException) =>
            val now = nanoTime()
            val waited = Duration(now - startTime, NANOSECONDS)
            val waitedReadable = LoggerUtil.roundDurationForHumans(waited)
            log(
              if (waited >= warnAfter) Level.INFO else Level.DEBUG,
              s"Task $description still not completed after ${waitedReadable}. Continue waiting...",
            )
            val leftOver = timeout.minus(waited)
            retry(
              leftOver,
              if (waited < warnAfter)
                warnAfter - waited // this enables warning at the earliest time we are asked to warn
              else warnAfter / 2,
            )

          case Failure(exn) => Failure(exn)
        }

      } else {
        val stackTraces = StackTraceUtil.formatStackTrace(stackTraceFilter)
        val msg = s"Task $description did not complete within $timeout."
        log(Level.WARN, s"${msg} Stack traces:\n${stackTraces}")
        Failure(new TimeoutException(msg))
      }
    }

    retry(timeout, warnAfter)
  }
}
