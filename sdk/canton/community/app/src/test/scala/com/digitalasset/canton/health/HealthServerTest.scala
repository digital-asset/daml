// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.health.HealthServer.route
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
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
