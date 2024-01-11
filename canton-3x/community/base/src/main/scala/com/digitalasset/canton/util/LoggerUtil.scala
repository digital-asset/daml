// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

object LoggerUtil {

  /** Log a `message` at a given `level`.
    *
    * @param message The message to be logged. Call-by-name so that the message is computed only if the message is really logged.
    */
  def logAtLevel(level: Level, message: => String)(implicit
      loggingContext: ErrorLoggingContext
  ): Unit = {
    val logger = loggingContext.logger
    implicit val traceContext: TraceContext = loggingContext.traceContext
    level match {
      case Level.TRACE => logger.trace(message)
      case Level.DEBUG => logger.debug(message)
      case Level.INFO => logger.info(message)
      case Level.WARN => logger.warn(message)
      case Level.ERROR => logger.error(message)
    }
  }

  /** Log a `message` with a `throwable` at a given `level`. */
  def logThrowableAtLevel(level: Level, message: => String, throwable: => Throwable)(implicit
      loggingContext: ErrorLoggingContext
  ): Unit = {
    val logger = loggingContext.logger
    implicit val traceContext: TraceContext = loggingContext.traceContext
    level match {
      case Level.TRACE => logger.trace(message, throwable)
      case Level.DEBUG => logger.debug(message, throwable)
      case Level.INFO => logger.info(message, throwable)
      case Level.WARN => logger.warn(message, throwable)
      case Level.ERROR => logger.error(message, throwable)
    }
  }

  /** Log the time taken by a task `run` */
  def clue[T](message: => String)(run: => T)(implicit loggingContext: ErrorLoggingContext): T = {
    val logger = loggingContext.logger
    implicit val traceContext: TraceContext = loggingContext.traceContext
    logger.debug(s"Starting $message")
    val st = System.nanoTime()
    val ret = run
    val end = roundDurationForHumans(Duration(System.nanoTime() - st, TimeUnit.NANOSECONDS))
    logger.debug(s"Finished $message after $end")
    ret
  }

  /** Round a duration such that humans can easier graps the numbers
    *
    * Duration offers a method .toCoarsest that will figure out the coarsest
    * time unit. However, this method doesn't really do anything if we have nanoseconds
    * as it only truncates 0.
    *
    * Therefore, this method allows to set lower digits to 0 and only keep the leading digits as nonzeros.
    */
  def roundDurationForHumans(duration: Duration, keep: Int = 2): Duration = {
    if (duration.isFinite && duration.length != 0) {
      val length = Math.abs(duration.length)
      val adjusted = length - length % math
        .pow(10, Math.max(0, math.floor(math.log10(length.toDouble)) - keep))
      Duration(if (duration.length > 0) adjusted else -adjusted, duration.unit).toCoarsest
    } else duration
  }

  def logOnThrow[T](task: => T)(implicit loggingContext: ErrorLoggingContext): T = {
    try {
      task
    } catch {
      case NonFatal(e) =>
        loggingContext.error("Unhandled exception thrown!", e)
        throw e
    }
  }

  def logOnThrow_(task: => Unit)(implicit loggingContext: ErrorLoggingContext): Unit = {
    try {
      task
    } catch {
      case NonFatal(e) =>
        loggingContext.logger.error("Unhandled exception thrown!", e)(loggingContext.traceContext)
    }
  }

  /** truncates a string
    *
    * @param maxLines truncate after observing the given number of newline characters
    * @param maxSize truncate after observing the given number of characters
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

}
