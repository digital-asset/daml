// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.sampleservice

import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse}

import scala.concurrent.Future

trait HelloServiceResponding extends HelloService {

  override def single(request: HelloRequest): Future[HelloResponse] =
    Future.successful(response(request))

  override def fails(request: HelloRequest): Future[HelloResponse] =
    Future.failed(new IllegalStateException(request.payload.toStringUtf8))

  protected def response(request: HelloRequest): HelloResponse =
    HelloResponse(request.reqInt * 2, request.payload)

  protected def responses(request: HelloRequest): List[HelloResponse] =
    (1 to request.reqInt).map(i => HelloResponse(i, request.payload)).toList

}
