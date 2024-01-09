// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.sampleservice.implementations

import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition, Status}

import scala.concurrent.ExecutionContext

// TODO(#16172): Remove file once we can use the artifact from Daml SDK (sample-service)
@SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
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

  private def validateRequest(request: HelloRequest): Unit =
    if (request.reqInt < 0)
      throw Status.INVALID_ARGUMENT
        .withDescription("request cannot be negative")
        .asRuntimeException()

}
