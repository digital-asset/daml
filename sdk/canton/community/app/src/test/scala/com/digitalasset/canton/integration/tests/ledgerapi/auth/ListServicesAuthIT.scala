// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.test.StreamConsumer
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import io.grpc.reflection.v1.{ServerReflectionGrpc, ServerReflectionResponse}

import scala.concurrent.Future

class ListServicesAuthIT extends UnsecuredServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "ServerReflection#List"

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    new StreamConsumer[ServerReflectionResponse](observer =>
      stub(ServerReflectionGrpc.newStub(channel), context.token)
        .serverReflectionInfo(observer)
        .onCompleted()
    ).first()
  }
}
