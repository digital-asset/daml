// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.sampleservice.implementations

import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{
  HelloRequest,
  HelloRequestHeavy,
  HelloResponse,
  HelloResponseHeavy,
  HelloServiceGrpc,
}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition, Status}

import scala.concurrent.ExecutionContext

class HelloServiceReferenceImplementation
    extends HelloService
    with HelloServiceResponding
    with BindableService
    with AutoCloseable {

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    HelloServiceGrpc.bindService(this, ExecutionContext.global)

  override def serverStreaming(
      request: HelloRequest,
      responseObserver: StreamObserver[HelloResponse],
  ): Unit = {
    validateRequest(request)
    for (i <- 1.to(request.reqInt)) responseObserver.onNext(HelloResponse(i))
    responseObserver.onCompleted()
  }

  override def serverStreamingHeavy(
      request: HelloRequestHeavy,
      responseObserver: StreamObserver[HelloResponseHeavy],
  ): Unit = ???

  private def validateRequest(request: HelloRequest): Unit =
    if (request.reqInt < 0)
      throw Status.INVALID_ARGUMENT
        .withDescription("request cannot be negative")
        .asRuntimeException()

}
