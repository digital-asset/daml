// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class CreateCommand extends Simulation with SimulationConfig {

  private val jsonCommand = """{
  "templateId": "Iou:Iou",
  "payload": {
    "issuer": "Alice",
    "owner": "Alice",
    "currency": "USD",
    "amount": "9.99",
    "observers": []
  }
}"""

  private val request = http("CreateCommand")
    .post("/v1/create")
    .body(StringBody(jsonCommand))

  private val scn = scenario("CreateCommandScenario")
    .repeat(1000)(exec(request))

  setUp(
    scn.inject(atOnceUsers(1))
  ).protocols(httpProtocol)
}
