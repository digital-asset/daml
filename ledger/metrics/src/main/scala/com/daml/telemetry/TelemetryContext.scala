// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import java.util.{HashMap => jHashMap, Map => jMap}

import io.opentelemetry.api.trace.{Span, Tracer}
import io.opentelemetry.context.Context

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait TelemetryContext {

  /** Sets or replaces the value of `attribute` to `value`.
    */
  def setAttribute(attribute: SpanAttribute, value: String): TelemetryContext

  /** Creates a new span and runs the computation inside it.
    * The new span has its parent set as the span associated with the current context.
    * A new context containing the new span is passed as parameter to the computation.
    *
    * @param spanName   the name of the new span
    * @param kind       the kind of the new span
    * @param attributes the key-value pairs to be set as attributes to the new span
    * @param body       the computation to be run in the new span
    * @return the result of the computation
    */
  def runFutureInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  )(
      body: TelemetryContext => Future[T]
  ): Future[T]

  /** Creates a new span and runs the computation inside it.
    * The new span has its parent set as the span associated with the current context.
    * A new context containing the new span is passed as parameter to the computation.
    *
    * @param spanName the name of the new span
    * @param kind the kind of the new span
    * @param attributes the key-value pairs to be set as attributes to the new span
    * @param body the computation to be run in the new span
    * @return the result of the computation
    */
  def runInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  )(
      body: TelemetryContext => T
  ): T

  /** Runs the computation inside an OpenTelemetry scope.
    *
    * This is used to set the tracing metadata in the gRPC local thread context, so it
    * can be accessed and used by the OpenTelemetry instrumentation to create complete traces.
    *
    * It should be used around gRPC calls, to ensure that the tracing metadata is correctly used
    * and transferred.
    *
    * @param body the computation to be run in the Telemetry scope
    * @return the result of the computation
    */
  def runInOpenTelemetryScope[T](body: => T): T

  /** Encode the metadata of the context in a key-value map.
    *
    * Typically, metadata is encoded in a map to allow transporting it across process boundaries.
    * Originally, it has been created to carry tracing metadata across boundaries, and
    * to create complete traces.
    *
    * @see [[com.daml.telemetry.Telemetry.contextFromMetadata(java.util.Map)]]
    */
  def encodeMetadata(): jMap[String, String]

  /** Returns a raw Open Telemetry context.
    * Should only be used by consumers that are using the Open Telemetry API directly.
    *
    * @return Open Telemetry context
    */
  def openTelemetryContext: Context

}

/** Default implementation of TelemetryContext. Uses OpenTelemetry to generate and gather traces.
  */
protected class DefaultTelemetryContext(protected val tracer: Tracer, protected val span: Span)
    extends TelemetryContext {

  def setAttribute(attribute: SpanAttribute, value: String): TelemetryContext = {
    span.setAttribute(attribute.key, value)
    this
  }

  override def runFutureInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  )(
      body: TelemetryContext => Future[T]
  ): Future[T] = {
    val subSpan = createSubSpan(spanName, kind, attributes: _*)

    val result = body(DefaultTelemetryContext(tracer, subSpan))
    result.andThen {
      case Failure(t) =>
        subSpan.recordException(t)
        subSpan.end()
      case Success(_) =>
        subSpan.end()
    }(ExecutionContext.parasitic)
  }

  override def runInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  )(
      body: TelemetryContext => T
  ): T = {
    val subSpan = createSubSpan(spanName, kind, attributes: _*)

    try {
      body(DefaultTelemetryContext(tracer, subSpan))
    } catch {
      case exception: Exception =>
        subSpan.recordException(exception)
        throw exception
    } finally {
      subSpan.end()
    }
  }

  protected def createSubSpan(
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  ): Span = {
    val subSpan =
      tracer
        .spanBuilder(spanName)
        .setParent(openTelemetryContext)
        .setSpanKind(kind.kind)
        .startSpan()
    for {
      (attribute, value) <- attributes
    } {
      subSpan.setAttribute(attribute.key, value)
    }
    subSpan
  }

  override def runInOpenTelemetryScope[T](body: => T): T = {
    val scope = openTelemetryContext.makeCurrent()
    try {
      body
    } finally {
      scope.close()
    }
  }

  override def encodeMetadata(): jMap[String, String] = {
    import scala.jdk.CollectionConverters._
    Tracing.encodeTraceMetadata(openTelemetryContext).asJava
  }

  override def openTelemetryContext: Context = Context.current.`with`(span)
}

object DefaultTelemetryContext {
  def apply(tracer: Tracer, span: Span): DefaultTelemetryContext =
    new DefaultTelemetryContext(tracer, span)
}

protected class RootDefaultTelemetryContext(override protected val tracer: Tracer)
    extends DefaultTelemetryContext(tracer, Span.getInvalid) {
  override protected def createSubSpan(
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  ): Span = {
    val subSpan =
      tracer.spanBuilder(spanName).setNoParent().setSpanKind(kind.kind).startSpan()
    for {
      (attribute, value) <- attributes
    } {
      subSpan.setAttribute(attribute.key, value)
    }
    subSpan
  }
}

object RootDefaultTelemetryContext {
  def apply(tracer: Tracer): RootDefaultTelemetryContext =
    new RootDefaultTelemetryContext(tracer)
}

/** Implementation of Telemetry that does nothing.
  *
  * It always returns NoOpTelemetryContext, and just executes without modification any given code block function.
  */
object NoOpTelemetryContext extends TelemetryContext {

  def setAttribute(attribute: SpanAttribute, value: String): TelemetryContext = this

  override def runFutureInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  )(
      body: TelemetryContext => Future[T]
  ): Future[T] = {
    body(this)
  }

  override def runInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (SpanAttribute, String)*
  )(
      body: TelemetryContext => T
  ): T = {
    body(this)
  }

  override def runInOpenTelemetryScope[T](body: => T): T = {
    body
  }

  override def encodeMetadata(): jMap[String, String] = new jHashMap()

  override def openTelemetryContext: Context = Context.root.`with`(Span.getInvalid)
}
