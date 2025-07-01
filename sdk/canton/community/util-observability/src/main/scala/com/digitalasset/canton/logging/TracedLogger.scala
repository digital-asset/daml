// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.scalalogging.{CanLog, Logger}
import org.slf4j
import org.slf4j.MDC

/** Set a trace-id in MDC before logging a message and clear immediately afterwards */
private[logging] case object CanLogTraceContext extends CanLog[TraceContext] {
  private[logging] val traceIdMdcKey = "trace-id"
  private[logging] val spanIdMdcKey = "span-id"
  private[logging] val spanParentIdMdcKey = "span-parent-id"
  private[logging] val spanNameMdcKey = "span-name"

  override def logMessage(msg: String, context: TraceContext): String = {
    context.traceId.foreach(MDC.put(traceIdMdcKey, _))
    context.spanParentId.foreach(MDC.put(spanParentIdMdcKey, _))
    context.spanId.foreach(MDC.put(spanIdMdcKey, _))
    context.spanName.foreach(MDC.put(spanNameMdcKey, _))
    msg
  }

  override def afterLog(context: TraceContext): Unit = {
    MDC.remove(traceIdMdcKey)
    MDC.remove(spanParentIdMdcKey)
    MDC.remove(spanIdMdcKey)
    MDC.remove(spanNameMdcKey)
  }
}

object TracedLogger {
  private implicit val canLogTraceContext: CanLog[TraceContext] = CanLogTraceContext

  def apply(logger: slf4j.Logger): TracedLogger = Logger.takingImplicit[TraceContext](logger)
  def apply(logger: Logger): TracedLogger = apply(logger.underlying)
  def apply(klass: Class[_], loggerFactory: NamedLoggerFactory): TracedLogger = apply(
    loggerFactory.getLogger(klass)
  )
}
