// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import java.util.{HashMap => jHashMap, Map => jMap}

import com.daml.dec.DirectExecutionContext
import io.grpc.MethodDescriptor
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait TelemetryContext {

  /** Sets or replaces the value of attribute `key` to `value`.
    */
  def setAttribute[T](key: AttributeKey[T], value: T): TelemetryContext

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
      attributes: (AttributeKey[String], String)*
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
      attributes: (AttributeKey[String], String)*
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

}

object TelemetryContext {

  /** Sets the attributes extracted from the given gRPC `MethodDescriptor`.
    */
  def setGrpcAttributes(
      methodDescriptor: MethodDescriptor[_, _]
  )(implicit telemetryContext: TelemetryContext): Unit = {
    telemetryContext.setAttribute(SemanticAttributes.RPC_SYSTEM, "grpc")
    telemetryContext.setAttribute(
      SemanticAttributes.RPC_SERVICE,
      methodDescriptor.getServiceName,
    )
    ()
  }
}

/** Default implementation of TelemetryContext. Uses OpenTelemetry to generate and gather traces.
  */
protected class DefaultTelemetryContext(protected val span: Span) extends TelemetryContext {

  def setAttribute[T](key: AttributeKey[T], value: T): TelemetryContext = {
    span.setAttribute(key, value)
    this
  }

  override def runFutureInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (AttributeKey[String], String)*
  )(
      body: TelemetryContext => Future[T]
  ): Future[T] = {
    val subSpan = createSubSpan(spanName, kind, attributes: _*)

    val result = body(DefaultTelemetryContext(subSpan))
    result.onComplete {
      case Failure(t) =>
        subSpan.recordException(t)
        subSpan.end()
      case Success(_) =>
        subSpan.end()
    }(DirectExecutionContext)
    result
  }

  override def runInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (AttributeKey[String], String)*
  )(
      body: TelemetryContext => T
  ): T = {
    val subSpan = createSubSpan(spanName, kind, attributes: _*)

    try {
      body(DefaultTelemetryContext(subSpan))
    } finally {
      subSpan.end()
    }
  }

  protected def createSubSpan(
      spanName: String,
      kind: SpanKind,
      attributes: (AttributeKey[String], String)*
  ): Span = {
    val subSpan =
      ParticipantTracer
        .spanBuilder(spanName)
        .setParent(Context.current.`with`(span))
        .setSpanKind(kind.kind)
        .startSpan()
    for {
      (key, value) <- attributes
    } {
      subSpan.setAttribute(key, value)
    }
    subSpan
  }

  override def runInOpenTelemetryScope[T](body: => T): T = {
    val scope = Context.current.`with`(span).makeCurrent()
    try {
      body
    } finally {
      scope.close()
    }
  }

  override def encodeMetadata(): jMap[String, String] = {
    import scala.jdk.CollectionConverters._
    Tracing.encodeTraceMetadata(Context.current.`with`(span)).asJava
  }
}

object DefaultTelemetryContext {
  def apply(span: Span): DefaultTelemetryContext =
    new DefaultTelemetryContext(span)
}

protected object RootDefaultTelemetryContext extends DefaultTelemetryContext(Span.getInvalid) {
  override protected def createSubSpan(
      spanName: String,
      kind: SpanKind,
      attributes: (AttributeKey[String], String)*
  ): Span = {
    val subSpan =
      ParticipantTracer.spanBuilder(spanName).setNoParent().setSpanKind(kind.kind).startSpan()
    for {
      (key, value) <- attributes
    } {
      subSpan.setAttribute(key, value)
    }
    subSpan
  }
}

/** Implementation of Telemetry that does nothing.
  *
  * It always returns NoOpTelemetryContext, and just executes without modification any given code block function.
  */
object NoOpTelemetryContext extends TelemetryContext {

  def setAttribute[T](key: AttributeKey[T], value: T): TelemetryContext = this

  override def runFutureInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (AttributeKey[String], String)*
  )(
      body: TelemetryContext => Future[T]
  ): Future[T] = {
    body(this)
  }

  override def runInNewSpan[T](
      spanName: String,
      kind: SpanKind,
      attributes: (AttributeKey[String], String)*
  )(
      body: TelemetryContext => T
  ): T = {
    body(this)
  }

  override def runInOpenTelemetryScope[T](body: => T): T = {
    body
  }

  override def encodeMetadata(): jMap[String, String] = new jHashMap()
}
