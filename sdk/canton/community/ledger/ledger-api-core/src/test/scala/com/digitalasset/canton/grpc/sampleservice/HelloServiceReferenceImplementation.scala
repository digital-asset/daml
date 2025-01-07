// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.grpc.sampleservice

import com.digitalasset.canton.protobuf
import com.digitalasset.canton.protobuf.HelloServiceGrpc.HelloService
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.{ExecutionContext, Future}

class HelloServiceReferenceImplementation(implicit ec: ExecutionContext)
    extends HelloService
    with BindableService {

  override def bindService(): ServerServiceDefinition =
    protobuf.HelloServiceGrpc.bindService(this, ec)

  override def helloStreamed(
      request: protobuf.Hello.Request,
      responseObserver: StreamObserver[protobuf.Hello.Response],
  ): Unit = {
    responseObserver.onNext(protobuf.Hello.Response(request.msg))
    responseObserver.onCompleted()
  }

  override def hello(request: protobuf.Hello.Request): Future[protobuf.Hello.Response] =
    Future.successful(protobuf.Hello.Response(request.msg * 2))
}
