// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.health.HealthServer.route
import org.scalatest.wordspec.AnyWordSpec

class HealthServerTest extends AnyWordSpec with BaseTest with ScalatestRouteTest {
  "HealthServer" should {
    "return 200 if the check is healthy" in
      Get("/health") ~> route(StaticHealthCheck(Healthy)) ~> check {
        status shouldBe StatusCodes.OK
      }

    "return 500 if the check is unhealthy" in
      Get("/health") ~> route(StaticHealthCheck(Unhealthy(":("))) ~> check {
        status shouldBe StatusCodes.InternalServerError
        responseAs[String] shouldBe ":("
      }
  }
}
