// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.ErrorCode.formatContextAsString
import com.daml.error.ContextualizedErrorLogger.loggingValueToString
import com.daml.logging.entries.LoggingValue
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import org.slf4j.event.Level

/** Abstracts away from the logging tech stack used. */
trait ContextualizedErrorLogger {
  def properties: Map[String, String]
  def correlationId: Option[String]
  def logError(err: BaseError, extra: Map[String, String]): Unit
  def info(message: String): Unit
  def info(message: String, throwable: Throwable): Unit
  def warn(message: String): Unit
  def warn(message: String, throwable: Throwable): Unit
  def error(message: String): Unit
  def error(message: String, throwable: Throwable): Unit
}

object DamlContextualizedErrorLogger {

  def forClass(
      clazz: Class[_],
      loggingContext: LoggingContext = LoggingContext.empty,
      correlationId: Option[String] = None,
  ): DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(
      ContextualizedLogger.get(clazz),
      loggingContext,
      correlationId,
    )

  def forTesting(clazz: Class[_], correlationId: Option[String] = None) =
    new DamlContextualizedErrorLogger(
      ContextualizedLogger.get(clazz),
      LoggingContext.ForTesting,
      correlationId,
    )
}

/** Implementation of [[ContextualizedErrorLogger]] leveraging the //libs-scala/contextualized-logging
  * as the logging stack.
  *
  * @param logger The logger.
  * @param loggingContext The logging context.
  * @param correlationId The correlation id, if present. The choice of the correlation id depends on the
  *                      ledger integration. By default it should be the command submission id.
  */
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
        case (Level.ERROR, None) => logger.error(message)
        case (Level.ERROR, Some(tr)) => logger.error(message, tr)
        case (Level.WARN, None) => logger.warn(message)
        case (Level.WARN, Some(tr)) => logger.warn(message, tr)
        case (Level.INFO, None) => logger.info(message)
        case (Level.INFO, Some(tr)) => logger.info(message, tr)
        case (Level.DEBUG, None) => logger.debug(message)
        case (Level.DEBUG, Some(tr)) => logger.debug(message, tr)
        case (Level.TRACE, None) => logger.trace(message)
        case (Level.TRACE, Some(tr)) => logger.trace(message, tr)
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

object NoLogging extends ContextualizedErrorLogger {
  override def properties: Map[String, String] = Map.empty
  override def correlationId: Option[String] = None
  override def logError(err: BaseError, extra: Map[String, String]): Unit = ()
  override def info(message: String): Unit = ()
  override def info(message: String, throwable: Throwable): Unit = ()
  override def warn(message: String): Unit = ()
  override def warn(message: String, throwable: Throwable): Unit = ()
  override def error(message: String): Unit = ()
  override def error(message: String, throwable: Throwable): Unit = ()
}
