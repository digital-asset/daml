package com.daml.http.tracing

import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc._
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.{Context => OpenTelemetryContext}
import io.opentelemetry.context.propagation.TextMapSetter

object TraceContextGrpc {
  def clientInterceptor: ClientInterceptor = new TraceContextClientInterceptor

  private class TraceContextClientInterceptor extends ClientInterceptor {
    override def interceptCall[ReqT, RespT](
        method: MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions,
        next: Channel,
    ): ClientCall[ReqT, RespT] =
      new SimpleForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions)) {
        override def start(
            responseListener: ClientCall.Listener[RespT],
            headers: Metadata,
        ): Unit = {
          val propagator = W3CTraceContextPropagator.getInstance()
          propagator.inject(OpenTelemetryContext.current(), headers, grpcMetadataSetter)

          super.start(responseListener, headers)
        }
      }
  }

  private val grpcMetadataSetter: TextMapSetter[Metadata] =
    (carrier: Metadata, key: String, value: String) =>
      carrier.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value)

}
