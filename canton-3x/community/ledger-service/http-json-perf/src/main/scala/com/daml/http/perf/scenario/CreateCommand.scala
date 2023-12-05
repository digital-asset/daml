// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class CreateCommand extends Simulation with SimulationConfig {

  private val jsonCommand = s"""{
  "templateId": "Iou:Iou",
  "payload": {
    "issuer": "$aliceParty",
    "owner": "$aliceParty",
    "currency": "USD",
    "amount": "9.99",
    "observers": []
  }
}"""

  private val numberOfRuns = 1000
  private val request = http("CreateCommand")
    .post("/v1/create")
    .body(StringBody(jsonCommand))

  private val scn = scenario("CreateCommandScenario")
    .repeat(numberOfRuns / defaultNumUsers)(exec(request))

  setUp(
    scn.inject(atOnceUsers(defaultNumUsers))
  ).protocols(httpProtocol)
}
