// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

package com.daml.metrics

// import java.util.{Map => jMap}
import io.opentelemetry.trace.TracingContextUtils
import scala.concurrent.Future

trait Telemetry {

  /** Returns a telemetry context from the OpenTelemetry context stored in the gRPC
    * thread local context.
    * This is used to recover the tracing metadata from an incoming gRPC call,
    * and used for the subsequent spans.
    *
    * Current implementation only works with OpenTelemetry < 0.10. Later versions of
    * OpenTelemetry use a different context object.
    */
  def contextFromGrpcThreadLocalContext(): TelemetryContext

  // /** Returns a telemetry context from the metadata encoded in the given key-value map.
  //   *
  //   * Typically, the metadata map has been encoded in a message, which has been sent
  //   * across boundaries, from a different process.
  //   * Originally, it has been created to carry tracing metadata across boundaries, and
  //   * to create complete traces.
  //   *
  //   * @see [[com.daml.ledger.telemetry.TelemetryContext.encodeMetadata()]]
  //   */
  // def contextFromMetadata(metadata: Option[jMap[String, String]]): TelemetryContext

  /** Creates the first span of a new trace, and runs the computation inside it.
    *
    * @param spanName the name of the new span.
    * @param kind the kind of the new span.
    * @param attributes the key-value pairs to be set as attributes to the new span.
    * @param body the code to be run in the new span.
    * @return the context based on the new span.
    */
  def runFutureInSpan[T](spanName: String, kind: SpanKind, attributes: (String, String)*)(
      body: TelemetryContext => Future[T]
  ): Future[T] = {
    rootContext.runFutureInNewSpan(spanName, kind, attributes: _*)(body)
  }

  def runFutureInSpan[T](spanName: String, attributes: (String, String)*)(body: TelemetryContext => Future[T]): Future[T] = {
    runFutureInSpan(spanName, SpanKind.Internal, attributes: _*)(body)
  }

  /** Creates the first span of a new trace, and runs the computation inside it.
    *
    * @param spanName the name of the new span.
    * @param kind the kind of the new span.
    * @param attributes the key-value pairs to be set as attributes to the new span.
    * @param body the code to be run in the new span.
    * @return the context based on the new span.
    */
  def runInSpan[T](spanName: String, kind: SpanKind, attributes: (String, String)*)(
      body: TelemetryContext => T
  ): T = {
    rootContext.runInNewSpan(spanName, kind, attributes: _*)(body)
  }

  def runInSpan[T](spanName: String, attributes: (String, String)*)(
      body: TelemetryContext => T
  ): T = {
    runInSpan(spanName, SpanKind.Internal, attributes: _*)(body)
  }

  /** Returns a new root context for this implementation of Telemetry.
    *
    * Any span create from this context should be the first span of a new trace.
    */
  protected def rootContext: TelemetryContext
}

/** Default implementation of Telemetry. Uses OpenTelemetry to generate and gather traces.
  */
object DefaultTelemetry extends Telemetry {

  override def contextFromGrpcThreadLocalContext(): TelemetryContext = {
    DefaultTelemetryContext(TracingContextUtils.getCurrentSpan)
  }

  // override def contextFromMetadata(metadata: Option[jMap[String, String]]): TelemetryContext = {
  //   metadata.flatMap(Tracing.decodeTraceMetadata) match {
  //     case None =>
  //       RootDefaultTelemetryContext
  //     case Some(context) =>
  //       val span = TracingContextUtils.getSpan(context)
  //       DefaultTelemetryContext(span)
  //   }
  // }

  override protected def rootContext: TelemetryContext = RootDefaultTelemetryContext
}

/** Implementation of Telemetry that does nothing.
  *
  * It always returns NoOpTelemetryContext, and just executes without modification any given code block function.
  */
object NoOpTelemetry extends Telemetry {

  override def contextFromGrpcThreadLocalContext(): TelemetryContext = NoOpTelemetryContext

  // override def contextFromMetadata(metadata: Option[jMap[String, String]]): TelemetryContext =
  //   NoOpTelemetryContext

  override protected def rootContext: TelemetryContext = NoOpTelemetryContext
}
