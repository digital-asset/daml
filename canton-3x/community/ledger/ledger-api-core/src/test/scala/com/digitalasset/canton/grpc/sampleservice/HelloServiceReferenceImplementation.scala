// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.grpc.sampleservice

import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.api.v0.HelloServiceGrpc.HelloService
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.{ExecutionContext, Future}

class HelloServiceReferenceImplementation(implicit ec: ExecutionContext)
    extends HelloService
    with BindableService {

  override def bindService(): ServerServiceDefinition =
    v0.HelloServiceGrpc.bindService(this, ec)

  override def helloStreamed(
      request: v0.Hello.Request,
      responseObserver: StreamObserver[v0.Hello.Response],
  ): Unit = {
    responseObserver.onNext(v0.Hello.Response(request.msg))
    responseObserver.onCompleted()
  }

  override def hello(request: v0.Hello.Request): Future[v0.Hello.Response] =
    Future.successful(v0.Hello.Response(request.msg * 2))
}
