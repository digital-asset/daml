// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tracing

import java.util.concurrent.TimeUnit
import java.util.{Map => jMap}

import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.{OpenTelemetry, trace}
import io.opentelemetry.api.trace.{Span, SpanBuilder, SpanContext, Tracer}
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
    * @see [[com.daml.tracing.TelemetryContext.encodeMetadata()]]
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

  /** Returns the trace-id of the telemetry context from the OpenTelemetry context stored in the gRPC
    * thread local context.
    * This is used to obtain the tracing id from an incoming gRPC call.
    */
  def traceIdFromGrpcContext: Option[String] = contextFromGrpcThreadLocalContext().traceId

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
class DefaultOpenTelemetry(openTelemetry: OpenTelemetry)
    extends DefaultTelemetry(openTelemetry.getTracer(DamlTracerName))

/** Implementation of Telemetry that does nothing.
  *
  * It always returns NoOpTelemetryContext, and just executes without modification any given code block function.
  */
object NoOpTelemetry extends Telemetry(NoOpTracer) {

  override def contextFromGrpcThreadLocalContext(): TelemetryContext = NoOpTelemetryContext

  override def contextFromMetadata(metadata: Option[jMap[String, String]]): TelemetryContext =
    NoOpTelemetryContext

  override protected def rootContext: TelemetryContext = NoOpTelemetryContext

  override def contextFromOpenTelemetryContext(context: Context): TelemetryContext =
    NoOpTelemetryContext
}

private object NoOpTracer extends Tracer {
  override def spanBuilder(spanName: String): SpanBuilder = NoOpSpanBuilder

  private object NoOpSpanBuilder extends SpanBuilder {
    override def setParent(context: Context): SpanBuilder = this

    override def setNoParent(): SpanBuilder = this

    override def addLink(spanContext: SpanContext): SpanBuilder = this

    override def addLink(spanContext: SpanContext, attributes: Attributes): SpanBuilder = this

    override def setAttribute(key: String, value: String): SpanBuilder = this

    override def setAttribute(key: String, value: Long): SpanBuilder = this

    override def setAttribute(key: String, value: Double): SpanBuilder = this

    override def setAttribute(key: String, value: Boolean): SpanBuilder = this

    override def setAttribute[T](key: AttributeKey[T], value: T): SpanBuilder = this

    override def setSpanKind(spanKind: trace.SpanKind): SpanBuilder = this

    override def setStartTimestamp(startTimestamp: Long, unit: TimeUnit): SpanBuilder = this

    override def startSpan(): Span = Span.getInvalid
  }
}
