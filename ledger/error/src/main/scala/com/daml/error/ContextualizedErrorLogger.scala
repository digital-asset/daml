// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.ErrorCode.formatContextAsString
import com.daml.error.ContextualizedErrorLogger.loggingValueToString
import com.daml.logging.entries.LoggingValue
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import org.slf4j.event.Level

/** Abstracts away from the logging tech stack used. */
trait ContextualizedErrorLogger extends CanLog {
  def properties: Map[String, String]
  def correlationId: Option[String]
  def logError(err: BaseError, extra: Map[String, String]): Unit
}

trait CanLog {
  def info(message: String): Unit
  def info(message: String, throwable: Throwable): Unit

  def warn(message: String): Unit
  def warn(message: String, throwable: Throwable): Unit

  def error(message: String): Unit
  def error(message: String, throwable: Throwable): Unit
}

class DamlContextualizedErrorLogger(
    logger: ContextualizedLogger,
    loggingContext: LoggingContext,
    val correlationId: Option[String],
) extends ContextualizedErrorLogger {
  override def properties: Map[String, String] =
    loggingContext.entries.contents.view.map { case (key, value) =>
      key -> loggingValueToString(value)
    }.toMap

  def info(message: String): Unit = logger.info(message)(loggingContext)
  def info(message: String, throwable: Throwable): Unit =
    logger.info(message, throwable)(loggingContext)
  def warn(message: String): Unit = logger.warn(message)(loggingContext)
  def warn(message: String, throwable: Throwable): Unit =
    logger.warn(message, throwable)(loggingContext)
  def error(message: String): Unit = logger.error(message)(loggingContext)
  def error(message: String, throwable: Throwable): Unit =
    logger.error(message, throwable)(loggingContext)

  def logError(err: BaseError, extra: Map[String, String]): Unit = {
    val errorCode = err.code
    val logLevel = errorCode.logLevel
    val mergedContext = err.context ++ err.location.map(("location", _)).toList.toMap ++ extra

    LoggingContext.withEnrichedLoggingContext(
      "err-context" -> ("{" + formatContextAsString(mergedContext) + "}")
    ) { implicit loggingContext =>
      val message = errorCode.toMsg(err.cause, correlationId)
      (logLevel, err.throwableO) match {
        case (Level.INFO, None) => logger.info(message)
        case (Level.INFO, Some(tr)) => logger.info(message, tr)
        case (Level.WARN, None) => logger.warn(message)
        case (Level.WARN, Some(tr)) => logger.warn(message, tr)
        // TODO error codes: Handle below INFO levels explicitly
        // an error that is logged with < INFO is not an error ...
        case (_, None) => logger.error(message)
        case (_, Some(tr)) => logger.error(message, tr)
      }
    }(loggingContext)
  }
}

object ContextualizedErrorLogger {
  // TODO error-codes: Extract this function into `LoggingContext` companion (and test)
  private[error] val loggingValueToString: LoggingValue => String = {
    case LoggingValue.Empty => ""
    case LoggingValue.False => "false"
    case LoggingValue.True => "true"
    case LoggingValue.OfString(value) => s"'$value'"
    case LoggingValue.OfInt(value) => value.toString
    case LoggingValue.OfLong(value) => value.toString
    case LoggingValue.OfIterable(sequence) =>
      sequence.map(loggingValueToString).mkString("[", ", ", "]")
    case LoggingValue.Nested(entries) =>
      entries.contents.view
        .map { case (key, value) => s"$key: ${loggingValueToString(value)}" }
        .mkString("{", ", ", "}")
    case LoggingValue.OfJson(json) => json.toString()
  }
}
