// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.*
import io.grpc.{Context as GrpcContext}

/** Support for propagating TraceContext values across GRPC boundaries.
  * Includes:
  *   - a client interceptor for setting context values when sending requests to a server
  *   - a server interceptor for receiving context values when receiving requests from a client
  */
object TraceContextGrpc {
  // value of trace context in the GRPC Context
  private val TraceContextKey =
    Context.keyWithDefault[TraceContext]("traceContext", TraceContext.empty)

  def fromGrpcContext: TraceContext = TraceContextKey.get()

  def withGrpcTraceContext[A](f: TraceContext => A): A = f(fromGrpcContext)

  def withGrpcContext[A](traceContext: TraceContext)(fn: => A): A = {
    val context = GrpcContext.current().withValue(TraceContextKey, traceContext)

    context.call(() => fn)
  }

  def clientInterceptor: ClientInterceptor = new TraceContextClientInterceptor
  def serverInterceptor: ServerInterceptor = new TraceContextServerInterceptor

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
          val traceContext = TraceContextKey.get()

          W3CTraceContext.injectIntoGrpcMetadata(traceContext, headers)

          super.start(responseListener, headers)
        }
      }
  }

  private class TraceContextServerInterceptor extends ServerInterceptor {
    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT],
    ): ServerCall.Listener[ReqT] = {
      val traceContext = W3CTraceContext.fromGrpcMetadata(headers)
      val context = GrpcContext
        .current()
        .withValue(TraceContextKey, traceContext)
      Contexts.interceptCall(context, call, headers, next)
    }
  }
}
