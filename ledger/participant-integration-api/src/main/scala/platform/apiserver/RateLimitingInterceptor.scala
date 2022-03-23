// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Status.Code
import io.grpc._
import io.grpc.protobuf.StatusProto

import java.util.concurrent.atomic.AtomicBoolean

private[apiserver] final class RateLimitingInterceptor() extends ServerInterceptor {

  val limit = new AtomicBoolean(false)

  def flip(): Boolean = limit.getAndSet(!limit.get())

  override def interceptCall[ReqT, RespT](
                                           call: ServerCall[ReqT, RespT],
                                           headers: Metadata,
                                           next: ServerCallHandler[ReqT, RespT],
                                         ): ServerCall.Listener[ReqT] = {
    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    if (fullMethodName == "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo") {
      next.startCall(call, headers)
    } else if (flip()) {
      println(s"RateLimiting Abort: $fullMethodName")

      val rpcStatus = com.google.rpc.Status
        .newBuilder()
        .setCode(Code.ABORTED.value())
        .setMessage("Call has been rate limited")
        .build()

      val exception = StatusProto.toStatusRuntimeException(rpcStatus)

      call.close(exception.getStatus, exception.getTrailers)
      new ServerCall.Listener[ReqT]() {}

    } else {
      println(s"RateLimiting Open: $fullMethodName")
      new RateLimitingListener(next.startCall(new RateLimitingServerCall(call), headers), fullMethodName)
    }
  }

  private final class RateLimitingListener[ReqT, RespT](delegate: ServerCall.Listener[ReqT], fullMethodName: String) extends SimpleForwardingServerCallListener[ReqT](delegate) {

    override def onMessage(message: ReqT): Unit = {
      println(s"> $fullMethodName ($message)")
      delegate.onMessage(message)
    }

  }


  private final class RateLimitingServerCall[ReqT, RespT](delegate: ServerCall[ReqT, RespT]) extends SimpleForwardingServerCall[ReqT, RespT](delegate) {

    private val fullMethodName = delegate.getMethodDescriptor.getFullMethodName

    override def sendMessage(message: RespT): Unit = {
      delegate.sendMessage(message)
      println(s"< $fullMethodName ($message)")
    }

    override def close(status: Status, trailers: Metadata): Unit = {
      delegate.close(status, trailers)
      println(s"RateLimiting Close: $fullMethodName ($status, $trailers)")
    }

  }

}

