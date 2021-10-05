// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.logging.{ContextualizedLogger, LoggingContext}

trait CanLog {
  def info(message: String): Unit
  def info(message: String, throwable: Throwable): Unit

  def warn(message: String): Unit
  def warn(message: String, throwable: Throwable): Unit

  def error(message: String): Unit
  def error(message: String, throwable: Throwable): Unit
}

trait ErrorCodeLoggingContext extends CanLog {
  def properties: Map[String, String]
  def correlationId: Option[String]
}

object DamlErrorCodeLoggingContext {
  implicit def asDamlErrorCodeLoggingContext(implicit
      loggingContext: LoggingContext,
      logger: ContextualizedLogger,
  ): DamlErrorCodeLoggingContext =
    // TODO error codes: figure whether we can provide something for that correlation id
    new DamlErrorCodeLoggingContext(logger, loggingContext, None)
}
class DamlErrorCodeLoggingContext(
    logger: ContextualizedLogger,
    loggingContext: LoggingContext,
    val correlationId: Option[String],
) extends ErrorCodeLoggingContext {
  override def properties: Map[String, String] =
    loggingContext.entries.contents.view.mapValues(_.toString).toMap

  override def info(message: String): Unit =
    logger.info(message)(loggingContext)

  override def info(message: String, throwable: Throwable): Unit =
    logger.info(message, throwable)(loggingContext)

  override def warn(message: String): Unit =
    logger.warn(message)(loggingContext)

  override def warn(message: String, throwable: Throwable): Unit =
    logger.warn(message, throwable)(loggingContext)

  override def error(message: String): Unit =
    logger.error(message)(loggingContext)

  override def error(message: String, throwable: Throwable): Unit =
    logger.error(message, throwable)(loggingContext)
}
