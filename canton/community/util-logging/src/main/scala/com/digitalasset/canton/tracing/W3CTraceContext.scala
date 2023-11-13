// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import org.apache.pekko.NotUsed
import io.grpc.Metadata
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.{TextMapGetter, TextMapSetter}
import io.opentelemetry.context.Context as OpenTelemetryContext

import java.lang
import scala.collection.mutable

/** Our representation of the w3c trace context values: https://www.w3.org/TR/trace-context/
  */
final case class W3CTraceContext(parent: String, state: Option[String] = None)
    extends Serializable {
  import W3CTraceContext.*

  def toTraceContext: TraceContext = W3CTraceContext.toTraceContext(Some(parent), state)

  /** HTTP headers of this trace context.
    * Marked transient as the headers do not need to be serialized when using java serialization.
    */
  @transient lazy val asHeaders: Map[String, String] =
    Map(TRACEPARENT_HEADER_NAME -> parent) ++ state.map(TRACESTATE_HEADER_NAME -> _).toList

  override def toString: String = {
    val sb = new mutable.StringBuilder()
    sb.append(this.getClass.getSimpleName)
    sb.append("(parent=")
    sb.append(parent)
    state.foreach(s => sb.append(", state=").append(s))
    sb.append(")")
    sb.result()
  }
}

object W3CTraceContext {
  // https://www.w3.org/TR/trace-context/
  private val propagator = W3CTraceContextPropagator.getInstance()
  private val TRACEPARENT_HEADER_NAME =
    "traceparent" // same as W3CTraceContextPropagator.TRACE_PARENT
  private val TRACESTATE_HEADER_NAME = "tracestate" // same as W3CTraceContextPropagator.TRACE_STATE

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def fromOpenTelemetryContext(context: OpenTelemetryContext): Option[W3CTraceContext] = {
    var builder = new W3CTraceContextBuilder
    val setter: TextMapSetter[W3CTraceContextBuilder] = (carrier, key, value) =>
      builder = key match {
        case TRACEPARENT_HEADER_NAME => carrier.copy(parent = Some(value))
        case TRACESTATE_HEADER_NAME => carrier.copy(state = Some(value))
        case _ => carrier
      }
    propagator.inject(context, builder, setter)
    builder.build
  }

  def fromHeaders(headers: Map[String, String]): Option[W3CTraceContext] =
    W3CTraceContextBuilder(
      headers.get(TRACEPARENT_HEADER_NAME),
      headers.get(TRACESTATE_HEADER_NAME),
    ).build

  private final case class W3CTraceContextBuilder(
      parent: Option[String] = None,
      state: Option[String] = None,
  ) {
    def build: Option[W3CTraceContext] = parent.map(W3CTraceContext(_, state))
  }

  def fromGrpcMetadata(metadata: Metadata): TraceContext =
    extract(metadata) { metadata => key =>
      metadata.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER))
    }

  def injectIntoGrpcMetadata(traceContext: TraceContext, metadata: Metadata): Unit =
    propagator.inject(traceContext.context, metadata, grpcMetadataSetter)

  /** Constructs a new trace context from optional serialized w3c trace context values.
    * If the values are missing or invalid to construct the span then a trace context will
    * still be returned but the current span will be invalid and [[TraceContext.traceId]]
    * will return None.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def toTraceContext(parent: Option[String], state: Option[String]): TraceContext = extract {
    case `TRACEPARENT_HEADER_NAME` => parent.orNull
    case `TRACESTATE_HEADER_NAME` => state.orNull
    case _ => null
  }

  private def extract(getter: String => String): TraceContext =
    extract(NotUsed)(_ => key => getter(key))

  private def extract[A](instance: A)(getter: A => String => String): TraceContext = {
    val openTelemetryContext = propagator.extract(
      OpenTelemetryContext.root(),
      instance,
      new SimpleGetter[A] {
        override def get(c: A, s: String): String = getter(c)(s)
      },
    )

    TraceContext(openTelemetryContext)
  }

  private val grpcMetadataSetter: TextMapSetter[Metadata] =
    (carrier: Metadata, key: String, value: String) =>
      carrier.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value)

  private abstract class SimpleGetter[A] extends TextMapGetter[A] {
    override def keys(c: A): lang.Iterable[String] = java.util.Collections.emptyList()
  }
}
