// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import com.digitalasset.grpc.adapter.server.rs.MockServerCallStreamObserver
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.server.api.services.grpc.GrpcHealthServiceSpec._
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.{Await, ExecutionContext}

object GrpcHealthServiceSpec {
  private val request = HealthCheckRequest()

  private val servingResponse = HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING)
}

final class GrpcHealthServiceSpec
    extends WordSpec
    with Matchers
    with Eventually
    with AkkaBeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  private implicit val executionContext: ExecutionContext = materializer.executionContext

  "HealthService" should {
    "check the current health" in {
      val service = new GrpcHealthService()

      val response = Await.result(service.check(request), patienceConfig.timeout)

      response should be(servingResponse)
    }

    "observe changes in health" in {
      val responseObserver = new MockServerCallStreamObserver[HealthCheckResponse]
      val service = new GrpcHealthService()

      service.watch(request, responseObserver)
      responseObserver.demandResponse()

      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }
    }
  }
}
