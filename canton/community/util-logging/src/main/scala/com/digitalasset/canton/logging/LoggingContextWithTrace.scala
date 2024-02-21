// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.daml.logging.LoggingContext
import com.daml.logging.entries.{LoggingEntries, LoggingEntry}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.tracing.TraceContext

/** Class to enrich [[com.digitalasset.canton.logging.ErrorLoggingContext]] with [[com.digitalasset.canton.tracing.TraceContext]]
  */
class LoggingContextWithTrace(
    override val entries: LoggingEntries,
    val traceContext: TraceContext,
) extends LoggingContext(entries) {
  def serializeFiltered(toInclude: String*): String =
    toPropertiesMap.filter(prop => toInclude.contains(prop._1)).mkString(", ")
}
object LoggingContextWithTrace {
  implicit def implicitExtractTraceContext(implicit source: LoggingContextWithTrace): TraceContext =
    source.traceContext

  val empty = LoggingContextWithTrace(TraceContext.empty)(LoggingContext.empty)

  def apply(telemetry: Telemetry)(implicit
      loggingContext: LoggingContext
  ): LoggingContextWithTrace = {
    val traceContext =
      TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
    new LoggingContextWithTrace(loggingContext.entries, traceContext)
  }

  val ForTesting: LoggingContextWithTrace =
    LoggingContextWithTrace.empty

  def apply(traceContext: TraceContext)(implicit
      loggingContext: LoggingContext
  ): LoggingContextWithTrace = {
    new LoggingContextWithTrace(loggingContext.entries, traceContext)
  }

  def apply(loggerFactory: NamedLoggerFactory)(implicit
      traceContext: TraceContext
  ): LoggingContextWithTrace = {
    new LoggingContextWithTrace(createLoggingContext(loggerFactory)(identity).entries, traceContext)
  }

  def apply(loggerFactory: NamedLoggerFactory, telemetry: Telemetry): LoggingContextWithTrace = {
    implicit val traceContext =
      TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
    LoggingContextWithTrace(loggerFactory)
  }

  /** ## Principles to follow when enriching the logging context
    *
    * ### Don't add values coming from a scope outside of the current method
    *
    * If a method receives a value as a parameter, it should trust that,
    * if it was relevant, the caller already added this value to the context.
    * Add values to the context as upstream as possible in the call chain.
    * This ensures to not add duplicates, possibly using slightly different
    * names to track the same value. The context was implemented to ensure
    * that values did not have to be passed down the entire call stack to
    * be logged at relevant points.
    *
    * ### Don't dump string representations of complex objects
    *
    * The purpose of the context is to be consumed by structured logging
    * frameworks. Dumping the string representation of an object, like a
    * Scala case class instance, means embedding some form of string
    * formatting in another (likely to be JSON). This can be difficult
    * to manage and parse, so stick to simple values (strings, numbers,
    * dates, etc.).
    */
  def withEnrichedLoggingContext[A](
      telemetry: Telemetry
  )(entry: LoggingEntry, entries: LoggingEntry*)(
      f: LoggingContextWithTrace => A
  )(implicit loggingContext: LoggingContext): A = {
    LoggingContext.withEnrichedLoggingContext(entry, entries*) { implicit loggingContext =>
      f(LoggingContextWithTrace(telemetry)(loggingContext))
    }
  }

  def withEnrichedLoggingContext[A](
      entry: LoggingEntry,
      entries: LoggingEntry*
  )(
      f: LoggingContextWithTrace => A
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): A = {
    LoggingContext.withEnrichedLoggingContext(entry, entries*) { implicit loggingContext =>
      f(LoggingContextWithTrace(loggingContextWithTrace.traceContext)(loggingContext))
    }
  }

  def enriched[A](
      entry: LoggingEntry,
      entries: LoggingEntry*
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): LoggingContextWithTrace =
    withEnrichedLoggingContext(entry, entries*)(identity)

  def withNewLoggingContext[A](entries: LoggingEntry*)(
      f: LoggingContextWithTrace => A
  )(implicit traceContext: TraceContext): A = {
    val loggingContextWithTrace: LoggingContextWithTrace =
      new LoggingContextWithTrace(LoggingEntries(entries*), traceContext)
    f(loggingContextWithTrace)
  }
}
