// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.test.StreamConsumer
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}

import scala.concurrent.Future

final class CheckHealthAuthIT extends UnsecuredServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "HealthService"

  private lazy val request = HealthCheckRequest.newBuilder().build()

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    new StreamConsumer[HealthCheckResponse](
      stub(HealthGrpc.newStub(channel), context.token).check(request, _)
    ).first()
  }
}
