// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.tracing.TraceContext

/** Enriches a [[com.digitalasset.canton.tracing.TraceContext]] with a [[NamedLoggerFactory]].
  * This allows to pass a [[NamedLoggerFactory]] into methods
  * that themselves do not have a [[NamedLoggerFactory]] in scope on their own, e.g.,
  * because they live inside a static object or in pure data classes.
  *
  * The logging methods of this class include the class's or object's name in the logger name.
  * To that end, the class or object should extend [[HasLoggerName]].
  * This ensures that the logger's name in the log file is close to where the log message was actually created.
  *
  * @see NamedLogging.namedLoggingContext for implicitly creating this from the [[NamedLoggerFactory]] of the caller
  * @see ErrorLoggingContext for a variant that fixes the logger name to the caller
  */
final case class NamedLoggingContext(
    loggerFactory: NamedLoggerFactory,
    traceContext: TraceContext,
) {

  /** Log something using the captured trace context */
  def trace(message: String)(implicit loggerNameFromClass: LoggerNameFromClass): Unit =
    tracedLogger.trace(message)(traceContext)
  def trace(message: String, throwable: Throwable)(implicit
      loggerNameFromClass: LoggerNameFromClass
  ): Unit =
    tracedLogger.trace(message, throwable)(traceContext)
  def debug(message: String)(implicit loggerNameFromClass: LoggerNameFromClass): Unit =
    tracedLogger.debug(message)(traceContext)
  def debug(message: String, throwable: Throwable)(implicit
      loggerNameFromClass: LoggerNameFromClass
  ): Unit =
    tracedLogger.debug(message, throwable)(traceContext)
  def info(message: String)(implicit loggerNameFromClass: LoggerNameFromClass): Unit =
    tracedLogger.info(message)(traceContext)
  def info(message: String, throwable: Throwable)(implicit
      loggerNameFromClass: LoggerNameFromClass
  ): Unit =
    tracedLogger.info(message, throwable)(traceContext)
  def warn(message: String)(implicit loggerNameFromClass: LoggerNameFromClass): Unit =
    tracedLogger.warn(message)(traceContext)
  def warn(message: String, throwable: Throwable)(implicit
      loggerNameFromClass: LoggerNameFromClass
  ): Unit =
    tracedLogger.warn(message, throwable)(traceContext)
  def error(message: String)(implicit loggerNameFromClass: LoggerNameFromClass): Unit =
    tracedLogger.error(message)(traceContext)
  def error(message: String, throwable: Throwable)(implicit
      loggerNameFromClass: LoggerNameFromClass
  ): Unit =
    tracedLogger.error(message, throwable)(traceContext)

  /** Log something using a different trace context */
  def tracedLogger(implicit loggerClass: LoggerNameFromClass): TracedLogger =
    TracedLogger(loggerClass.klass, loggerFactory)
}

final class LoggerNameFromClass(val klass: Class[_]) extends AnyVal

/** Defines implicits for obtaining the class used to create a logger out of a [[NamedLoggerFactory]]
  * in a [[NamedLoggingContext]].
  *
  * Extends [[scala.Any]] such that this can be used also for [[scala.AnyVal]] objects.
  */
trait HasLoggerName extends Any {
  protected implicit def loggerNameFromThisClass: LoggerNameFromClass =
    new LoggerNameFromClass(getClass)

  /** Convert a [[com.digitalasset.canton.logging.NamedLoggingContext]] into an
    * [[com.digitalasset.canton.logging.ErrorLoggingContext]]
    * to fix the logger name to the current class name.
    */
  protected implicit def errorLoggingContextFromNamedLoggingContext(implicit
      loggingContext: NamedLoggingContext
  ): ErrorLoggingContext =
    ErrorLoggingContext(
      loggingContext.tracedLogger,
      loggingContext.loggerFactory.properties,
      loggingContext.traceContext,
    )
}
