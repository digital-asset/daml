// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.platform.testing.StreamConsumer
import io.grpc.reflection.v1alpha.{ServerReflectionGrpc, ServerReflectionResponse}

import scala.concurrent.Future

class ListServicesAuthIT extends UnsecuredServiceCallAuthTests {
  override def serviceCallName: String = "ServerReflection#List"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    new StreamConsumer[ServerReflectionResponse](observer =>
      stub(ServerReflectionGrpc.newStub(channel), token)
        .serverReflectionInfo(observer)
        .onCompleted()
    ).first()
}
