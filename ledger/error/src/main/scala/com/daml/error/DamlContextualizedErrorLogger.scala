// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.logging.entries.{LoggingKey, LoggingValue}
import com.daml.logging.{ContextualizedLogger, LoggingContext, LoggingValueStringSerializer}
import org.slf4j.event.Level

import scala.collection.MapView

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
  * @param logger         The logger.
  * @param loggingContext The logging context.
  * @param correlationId  The correlation id, if present. The choice of the correlation id depends on the
  *                       ledger integration. By default it should be the command submission id.
  */
class DamlContextualizedErrorLogger(
    logger: ContextualizedLogger,
    loggingContext: LoggingContext,
    val correlationId: Option[String],
) extends ContextualizedErrorLogger {

  override def properties: Map[String, String] = {
    val a: MapView[LoggingKey, LoggingValue] = loggingContext.entries.contents.view
    a.map { case (key, value) =>
      key -> LoggingValueStringSerializer.makeString(value)
    }.toMap
  }

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
      "err-context" -> ("{" + ContextualizedErrorLogger.formatContextAsString(mergedContext) + "}")
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
