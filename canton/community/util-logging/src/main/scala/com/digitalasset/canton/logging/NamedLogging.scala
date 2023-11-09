// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.scalalogging.Logger
import org.slf4j
import org.slf4j.helpers.NOPLogger

trait NamedLogging {

  private implicit def canLogTraceContext = CanLogTraceContext

  protected implicit def errorLoggingContext(implicit
      traceContext: TraceContext
  ): ErrorLoggingContext =
    ErrorLoggingContext(theLoggerWithContext, loggerFactory.properties, traceContext)

  protected implicit def namedLoggingContext(implicit
      traceContext: TraceContext
  ): NamedLoggingContext =
    NamedLoggingContext(loggerFactory, traceContext)

  protected def loggerFactory: NamedLoggerFactory

  private[this] lazy val underlying: slf4j.Logger = theLogger.underlying

  private[this] lazy val theLogger: Logger = loggerFactory.getLogger(getClass)

  private[this] lazy val theLoggerWithContext: TracedLogger =
    Logger.takingImplicit[TraceContext](underlying)

  protected def noTracingLogger: Logger = theLogger
  protected def logger: TracedLogger = theLoggerWithContext
}

object NamedLogging {
  private implicit val canLogTraceContext = CanLogTraceContext

  def loggerWithoutTracing(logger: TracedLogger): Logger = Logger(logger.underlying)

  lazy val noopLogger: TracedLogger = Logger.takingImplicit[TraceContext](NOPLogger.NOP_LOGGER)
  lazy val noopNoTracingLogger: Logger = loggerWithoutTracing(noopLogger)
}
