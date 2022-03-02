// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import io.grpc.MethodDescriptor.MethodType
import io.grpc.{CallOptions, Channel, ClientCall, MethodDescriptor, ServerMethodDefinition}
import io.grpc.stub.{ClientCalls, ServerCalls, StreamObserver}

private[grpc] object ForwardCall {

  def apply[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      backend: Channel,
      options: CallOptions,
  ): ServerMethodDefinition[ReqT, RespT] = {
    val forward = () => backend.newCall(method, options)
    ServerMethodDefinition.create[ReqT, RespT](
      method,
      method.getType match {
        case MethodType.UNARY =>
          ServerCalls.asyncUnaryCall(new UnaryMethod(forward))
        case MethodType.CLIENT_STREAMING =>
          ServerCalls.asyncClientStreamingCall(new ClientStreamingMethod(forward))
        case MethodType.SERVER_STREAMING =>
          ServerCalls.asyncServerStreamingCall(new ServerStreamingMethod(forward))
        case MethodType.BIDI_STREAMING =>
          ServerCalls.asyncBidiStreamingCall(new BidiStreamMethod(forward))
        case MethodType.UNKNOWN =>
          sys.error(s"${method.getFullMethodName} has MethodType.UNKNOWN")
      },
    )
  }

  private final class UnaryMethod[ReqT, RespT](call: () => ClientCall[ReqT, RespT])
      extends ServerCalls.UnaryMethod[ReqT, RespT] {
    override def invoke(request: ReqT, responseObserver: StreamObserver[RespT]): Unit =
      ClientCalls.asyncUnaryCall(call(), request, responseObserver)
  }

  private final class ClientStreamingMethod[ReqT, RespT](call: () => ClientCall[ReqT, RespT])
      extends ServerCalls.ClientStreamingMethod[ReqT, RespT] {
    override def invoke(responseObserver: StreamObserver[RespT]): StreamObserver[ReqT] =
      ClientCalls.asyncClientStreamingCall(call(), responseObserver)
  }

  private final class ServerStreamingMethod[ReqT, RespT](call: () => ClientCall[ReqT, RespT])
      extends ServerCalls.ServerStreamingMethod[ReqT, RespT] {
    override def invoke(request: ReqT, responseObserver: StreamObserver[RespT]): Unit =
      ClientCalls.asyncServerStreamingCall(call(), request, responseObserver)
  }

  private final class BidiStreamMethod[ReqT, RespT](call: () => ClientCall[ReqT, RespT])
      extends ServerCalls.BidiStreamingMethod[ReqT, RespT] {
    override def invoke(responseObserver: StreamObserver[RespT]): StreamObserver[ReqT] =
      ClientCalls.asyncBidiStreamingCall(call(), responseObserver)
  }

}
