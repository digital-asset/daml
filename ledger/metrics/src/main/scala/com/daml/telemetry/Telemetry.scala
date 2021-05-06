// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import java.util.{Map => jMap}

import io.opentelemetry.api.trace.{Span, Tracer}
import io.opentelemetry.context.Context

import scala.concurrent.Future

/** @param tracer An OpenTelemetry Tracer that can be used for building spans. */
abstract class Telemetry(protected val tracer: Tracer) {

  /** Returns a telemetry context from the OpenTelemetry context stored in the gRPC
    * thread local context.
    * This is used to recover the tracing metadata from an incoming gRPC call,
    * and used for the subsequent spans.
    */
  def contextFromGrpcThreadLocalContext(): TelemetryContext

  /** Returns a telemetry context from the metadata encoded in the given key-value map.
    *
    * Typically, the metadata map has been encoded in a message, which has been sent
    * across boundaries, from a different process.
    * Originally, it has been created to carry tracing metadata across boundaries, and
    * to create complete traces.
    *
    * @see [[com.daml.telemetry.TelemetryContext.encodeMetadata()]]
    */
  def contextFromMetadata(metadata: Option[jMap[String, String]]): TelemetryContext

  /** Allows to transform an Open Telemetry context into a telemetry context.
    * Makes integration easier for consumers that are using the Open Telemetry API directly.
    * Consider using other factory methods.
    *
    * @param context raw Open Telemetry context
    */
  def contextFromOpenTelemetryContext(context: Context): TelemetryContext

  /** Creates the first span of a new trace, and runs the computation inside it.
    *
    * @param spanName the name of the new span.
    * @param kind the kind of the new span.
    * @param attributes the key-value pairs to be set as attributes to the new span.
    * @param body the code to be run in the new span.
    * @return the context based on the new span.
    */
  def runFutureInSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  )(
      body: TelemetryContext => Future[T]
  ): Future[T] = {
    rootContext.runFutureInNewSpan(spanName, kind, attributes: _*)(body)
  }

  /** Creates the first span of a new trace, and runs the computation inside it.
    *
    * @param spanName the name of the new span.
    * @param kind the kind of the new span.
    * @param attributes the key-value pairs to be set as attributes to the new span.
    * @param body the code to be run in the new span.
    * @return the context based on the new span.
    */
  def runInSpan[T](spanName: String, kind: SpanKind, attributes: (SpanAttribute, String)*)(
      body: TelemetryContext => T
  ): T = {
    rootContext.runInNewSpan(spanName, kind, attributes: _*)(body)
  }

  /** Returns a new root context for this implementation of Telemetry.
    *
    * Any span create from this context should be the first span of a new trace.
    */
  protected def rootContext: TelemetryContext
}

abstract class DefaultTelemetry(override protected val tracer: Tracer) extends Telemetry(tracer) {

  override def contextFromGrpcThreadLocalContext(): TelemetryContext = {
    DefaultTelemetryContext(tracer, Span.current)
  }

  override def contextFromMetadata(metadata: Option[jMap[String, String]]): TelemetryContext = {
    metadata
      .flatMap(Tracing.decodeTraceMetadata) match {
      case None =>
        RootDefaultTelemetryContext(tracer)
      case Some(context) =>
        DefaultTelemetryContext(tracer, Span.fromContext(context))
    }
  }

  override def contextFromOpenTelemetryContext(context: Context): TelemetryContext =
    Option(Span.fromContextOrNull(context))
      .map(span => DefaultTelemetryContext(tracer, span))
      .getOrElse(rootContext)

  override protected def rootContext: TelemetryContext = RootDefaultTelemetryContext(tracer)
}

/** Default implementation of Telemetry. Uses OpenTelemetry to generate and gather traces. */
object DefaultTelemetry extends DefaultTelemetry(OpenTelemetryTracer)

/** Implementation of Telemetry that does nothing.
  *
  * It always returns NoOpTelemetryContext, and just executes without modification any given code block function.
  */
object NoOpTelemetry extends Telemetry(Tracer.getDefault) {

  override def contextFromGrpcThreadLocalContext(): TelemetryContext = NoOpTelemetryContext

  override def contextFromMetadata(metadata: Option[jMap[String, String]]): TelemetryContext =
    NoOpTelemetryContext

  override protected def rootContext: TelemetryContext = NoOpTelemetryContext

  override def contextFromOpenTelemetryContext(context: Context): TelemetryContext =
    NoOpTelemetryContext
}
