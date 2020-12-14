// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

package com.daml.metrics

import io.opentelemetry.trace.Span
import scala.concurrent.Future
// import java.util.{Map => jMap, HashMap => jHashMap}
import scala.util.Failure
import scala.util.Success

import com.daml.dec.DirectExecutionContext
// import io.opentelemetry.trace.TracingContextUtils
// import io.grpc.Context
import io.opentelemetry.trace.DefaultSpan
// import io.grpc.MethodDescriptor
// import io.opentelemetry.trace.attributes.SemanticAttributes

trait TelemetryContext {

  /** Sets or replaces the value of attribute `key` to `value`.
    */
  def setAttribute(key: String, value: String): TelemetryContext

  /** Sets or replaces the value of attribute `key` to `value`.
    */
  def setAttribute(key: String, value: Long): TelemetryContext

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
  def runFutureInNewSpan[T](spanName: String, kind: SpanKind, attributes: (String, String)*)(
      body: TelemetryContext => Future[T]
  ): Future[T]

  def runFutureInNewSpan[T](spanName: String, attributes: (String, String)*)(   body: TelemetryContext => Future[T]
  ): Future[T] = runFutureInNewSpan(spanName, SpanKind.Internal, attributes: _*)(body)

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
  def runInNewSpan[T](spanName: String, kind: SpanKind, attributes: (String, String)*)(
      body: TelemetryContext => T
  ): T

  def runInNewSpan[T](spanName: String, attributes: (String, String)*)(   body: TelemetryContext => T
  ): T = runInNewSpan(spanName, SpanKind.Internal, attributes: _*)(body)

  // /** Runs the computation inside an OpenTelemetry scope.
  //   *
  //   * This is used to set the tracing metadata in the gRPC local thread context, so it
  //   * can be accessed and used by the OpenTelemetry instrumentation to create complete traces.
  //   *
  //   * It should be used around gRPC calls, to ensure that the tracing metadata is correctly used
  //   * and transferred.
  //   *
  //   * @param body the computation to be run in the Telemetry scope
  //   * @return the result of the computation
  //   */
  // def runInOpenTelemetryScope[T](body: => T): T

  // /** Encode the metadata of the context in a key-value map.
  //   *
  //   * Typically, metadata is encoded in a map to allow transporting it across process boundaries.
  //   * Originally, it has been created to carry tracing metadata across boundaries, and
  //   * to create complete traces.
  //   *
  //   * @see [[com.daml.ledger.telemetry.Telemetry.contextFromMetadata(java.util.Map)]]
  //   */
  // def encodeMetadata(): jMap[String, String]

}

object TelemetryContext {

  // /** Sets the attributes extracted from the given gRPC `MethodDescriptor`.
  //   */
  // def setGrpcAttributes(
  //     methodDescriptor: MethodDescriptor[_, _]
  // )(implicit telemetryContext: TelemetryContext): Unit = {
  //   telemetryContext.setAttribute(SemanticAttributes.RPC_SYSTEM.key, "grpc")
  //   telemetryContext.setAttribute(
  //     SemanticAttributes.RPC_SERVICE.key,
  //     methodDescriptor.getServiceName,
  //   )
  //   ()
  // }
}

/** Default implementation of TelemetryContext. Uses OpenTelemetry to generate and gather traces.
  */
case class DefaultTelemetryContext(span: Span) extends TelemetryContext {

  def setAttribute(key: String, value: String): TelemetryContext = {
    span.setAttribute(key, value)
    this
  }

  def setAttribute(key: String, value: Long): TelemetryContext = {
    span.setAttribute(key, value)
    this
  }

  def runFutureInNewSpan[T](spanName: String, kind: SpanKind, attributes: (String, String)*)(
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

  def runInNewSpan[T](spanName: String, kind: SpanKind, attributes: (String, String)*)(
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
      attributes: (String, String)*
  ): Span = {
    val subSpan =
      OpenTelemetryTracer.spanBuilder(spanName).setParent(span).setSpanKind(kind.kind).startSpan()
    for {
      (key, value) <- attributes
    } {
      subSpan.setAttribute(key, value)
    }
    subSpan
  }

  // def runInOpenTelemetryScope[T](body: => T): T = {
  //   val scope = TracingContextUtils.currentContextWith(span)
  //   try {
  //     body
  //   } finally {
  //     scope.close()
  //   }
  // }

  // def encodeMetadata(): jMap[String, String] = {
  //   import scala.collection.JavaConverters._
  //   val context = TracingContextUtils.withSpan(span, Context.ROOT)
  //   Tracing.encodeTraceMetadata(context).asJava
  // }
}

protected object RootDefaultTelemetryContext
    extends DefaultTelemetryContext(DefaultSpan.getInvalid) {
  override protected def createSubSpan(
      spanName: String,
      kind: SpanKind,
      attributes: (String, String)*
  ): Span = {
    val subSpan =
      OpenTelemetryTracer.spanBuilder(spanName).setNoParent().setSpanKind(kind.kind).startSpan()
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

  def setAttribute(key: String, value: String): TelemetryContext = this

  def setAttribute(key: String, value: Long): TelemetryContext = this

  def runFutureInNewSpan[T](spanName: String, kind: SpanKind, attributes: (String, String)*)(
      body: TelemetryContext => Future[T]
  ): Future[T] = {
    body(this)
  }

  def runInNewSpan[T](spanName: String, kind: SpanKind, attributes: (String, String)*)(
      body: TelemetryContext => T
  ): T = {
    body(this)
  }

  // def runInOpenTelemetryScope[T](body: => T): T = {
  //   body
  // }

  // def encodeMetadata(): jMap[String, String] = new jHashMap()
}
