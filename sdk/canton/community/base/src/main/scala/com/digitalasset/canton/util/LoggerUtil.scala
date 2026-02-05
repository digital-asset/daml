// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object LoggerUtil {

  /** Log a `message` at a given `level`.
    *
    * @param message
    *   The message to be logged. Call-by-name so that the message is computed only if the message
    *   is really logged.
    */
  def logAtLevel(level: Level, message: => String)(implicit
      loggingContext: ErrorLoggingContext
  ): Unit =
    level match {
      case Level.TRACE => loggingContext.trace(message)
      case Level.DEBUG => loggingContext.debug(message)
      case Level.INFO => loggingContext.info(message)
      case Level.WARN => loggingContext.warn(message)
      case Level.ERROR => loggingContext.error(message)
    }

  /** Log a `message` with a `throwable` at a given `level`. */
  def logThrowableAtLevel(level: Level, message: => String, throwable: => Throwable)(implicit
      loggingContext: ErrorLoggingContext
  ): Unit =
    level match {
      case Level.TRACE => loggingContext.trace(message, throwable)
      case Level.DEBUG => loggingContext.debug(message, throwable)
      case Level.INFO => loggingContext.info(message, throwable)
      case Level.WARN => loggingContext.warn(message, throwable)
      case Level.ERROR => loggingContext.error(message, throwable)
    }

  /** Log the time taken by a task `run` and optionally non-fatal throwable */
  def clue[T](message: => String, logNonFatalThrowable: Boolean = false)(
      run: => T
  )(implicit loggingContext: ErrorLoggingContext): T = {
    val logger = loggingContext.logger
    implicit val traceContext: TraceContext = loggingContext.traceContext
    logger.debug(s"Starting $message")
    val st = System.nanoTime()
    val ret = if (logNonFatalThrowable) logOnThrow(run) else run
    val delay = roundDurationForHumans(Duration(System.nanoTime() - st, TimeUnit.NANOSECONDS))
    logger.debug(s"Finished $message after $delay")
    ret
  }

  /** Log the time taken by a task `run` and optionally non-fatal throwable */
  def clueF[T](message: => String, logNonFatalThrowable: Boolean = false)(
      run: => Future[T]
  )(implicit loggingContext: ErrorLoggingContext, ec: ExecutionContext): Future[T] = {
    val logger = loggingContext.logger
    implicit val traceContext: TraceContext = loggingContext.traceContext
    logger.debug(s"Starting $message")
    val st = System.nanoTime()
    val ret = if (logNonFatalThrowable) logOnThrow(run) else run
    ret.onComplete { _ =>
      val delay = roundDurationForHumans(Duration(System.nanoTime() - st, TimeUnit.NANOSECONDS))
      logger.debug(s"Finished $message after $delay")

    }
    ret
  }

  def clueUSF[T](message: => String, logNonFatalThrowable: Boolean = false)(
      run: => FutureUnlessShutdown[T]
  )(implicit loggingContext: ErrorLoggingContext, ec: ExecutionContext): FutureUnlessShutdown[T] = {
    val logger = loggingContext.logger
    implicit val traceContext: TraceContext = loggingContext.traceContext
    logger.debug(s"Starting $message")
    val st = System.nanoTime()
    val ret = if (logNonFatalThrowable) logOnThrow(run) else run
    ret.onComplete { _ =>
      val delay = roundDurationForHumans(Duration(System.nanoTime() - st, TimeUnit.NANOSECONDS))
      logger.debug(s"Finished $message after $delay")

    }
    ret
  }

  /** Round a duration such that humans can grasp the numbers more easily, namely to the coarsest
    * unit (up to days) that retains at least 2 significant digits or does not lose any remaining
    * precision.
    */
  def roundDurationForHumans(duration: Duration): String =
    roundDurationForHumans(duration, keep = 2)

  /** Round a duration such that humans can grasp the numbers more easily, namely to the coarsest
    * unit (up to days) that retains at least `keep` many significant digits or does not lose any
    * remaining precision.
    */
  def roundDurationForHumans(duration: Duration, keep: Int): String =
    duration match {
      case finite: FiniteDuration =>
        import scala.jdk.DurationConverters.*
        roundDurationForHumans(finite.toJava, keep)
      case infinite => infinite.toString
    }

  /** Round a duration such that humans can grasp the numbers more easily, namely to the coarsest
    * unit (up to days) that retains at least 2 significant digits or does not lose any remaining
    * precision.
    */
  def roundDurationForHumans(duration: java.time.Duration): String =
    roundDurationForHumans(duration, keep = 2)

  /** Round a duration such that humans can grasp the numbers more easily, namely to the coarsest
    * unit (up to days) that retains at least `keep` many significant digits or does not lose any
    * remaining precision.
    */
  def roundDurationForHumans(duration: java.time.Duration, keep: Int): String =
    if (duration.isZero) "0 seconds"
    else {
      val keepBound = BigInt(10).pow(keep)

      @tailrec def go(length: BigInt, unit: TimeUnit): (BigInt, TimeUnit) = {
        val (coarser, divider) = unit match {
          case TimeUnit.NANOSECONDS => (TimeUnit.MICROSECONDS, 1000)
          case TimeUnit.MICROSECONDS => (TimeUnit.MILLISECONDS, 1000)
          case TimeUnit.MILLISECONDS => (TimeUnit.SECONDS, 1000)
          case TimeUnit.SECONDS => (TimeUnit.MINUTES, 60)
          case TimeUnit.MINUTES => (TimeUnit.HOURS, 60)
          case TimeUnit.HOURS => (TimeUnit.DAYS, 24)
          case TimeUnit.DAYS => (TimeUnit.DAYS, 1)
        }
        if (divider == 1) (length, unit)
        else {
          val scaled = length / divider
          if (scaled >= keepBound) go(scaled, coarser)
          else if (length % divider == 0) go(scaled, coarser)
          else (length, unit)
        }
      }

      val totalNanos =
        BigInt(duration.toSeconds) * BigInt(1_000_000_000L) + BigInt(duration.getNano)
      val (scaled, unit) = go(totalNanos, TimeUnit.NANOSECONDS)
      val singularUnitName = unit match {
        case TimeUnit.NANOSECONDS => "nano"
        case TimeUnit.MICROSECONDS => "microsecond"
        case TimeUnit.MILLISECONDS => "millisecond"
        case TimeUnit.SECONDS => "second"
        case TimeUnit.MINUTES => "minute"
        case TimeUnit.HOURS => "hour"
        case TimeUnit.DAYS => "day"
      }
      val unitName = if (scaled == 1) singularUnitName else singularUnitName + "s"
      scaled.toString() + " " + unitName
    }

  def logOnThrow[T](task: => T)(implicit loggingContext: ErrorLoggingContext): T =
    try {
      task
    } catch {
      case NonFatal(e) =>
        loggingContext.error("Unhandled exception thrown!", e)
        throw e
    }

  def logOnThrow_(task: => Unit)(implicit loggingContext: ErrorLoggingContext): Unit =
    try {
      task
    } catch {
      case NonFatal(e) =>
        loggingContext.error("Unhandled exception thrown!", e)
    }

  /** truncates a string
    *
    * @param maxLines
    *   truncate after observing the given number of newline characters
    * @param maxSize
    *   truncate after observing the given number of characters
    */
  def truncateString(maxLines: Int, maxSize: Int)(str: String): String = {
    val builder = new StringBuilder()
    val (lines, length) = str.foldLeft((0, 0)) {
      case ((lines, length), elem) if lines < maxLines && length < maxSize =>
        builder.append(elem)
        (lines + (if (elem == '\n') 1 else 0), length + 1)
      case (acc, _) => acc
    }
    val append = if (lines == maxLines || length == maxSize) " ..." else ""
    builder.toString + append
  }

  def limitForLogging[T](items: Iterable[T], maxItems: Int = 10): String =
    "[" + items.take(maxItems).mkString(", ") + (if (items.sizeIs > maxItems)
                                                   s", ... (total ${items.size} items)"
                                                 else "") + "]"

}
