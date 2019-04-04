// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.sampleservice

import com.digitalasset.platform.hello.HelloServiceGrpc.HelloService
import com.digitalasset.platform.hello.{HelloRequest, HelloResponse}

import scala.concurrent.Future

trait Responding extends HelloService {

  override def single(request: HelloRequest): Future[HelloResponse] =
    Future.successful(response(request))

  protected def response(request: HelloRequest) = HelloResponse(request.reqInt * 2, request.payload)

  protected def responses(request: HelloRequest) =
    (1 to request.reqInt).map(i => HelloResponse(i, request.payload)).toList

}
