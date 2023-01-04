// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.platform.testing.StreamConsumer
import io.grpc.health.v1.HealthGrpc
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse}

import scala.concurrent.Future

final class CheckHealthAuthIT extends UnsecuredServiceCallAuthTests {
  override def serviceCallName: String = "HealthService"

  private lazy val request = HealthCheckRequest.newBuilder().build()

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    new StreamConsumer[HealthCheckResponse](
      stub(HealthGrpc.newStub(channel), token).check(request, _)
    ).first()
}
