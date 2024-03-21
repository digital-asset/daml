// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class CreateAndExerciseCommand extends Simulation with SimulationConfig {

  private val jsonCommand = """{
  "templateId": "Iou:Iou",
  "payload": {
    "observers": [],
    "issuer": "Alice",
    "amount": "10.99",
    "currency": "USD",
    "owner": "Alice"
  },
  "choice": "Iou_Transfer",
  "argument": {
    "newOwner": "Bob"
  }
}"""

  private val numberOfRuns = 2000
  private val request = http("CreateAndExerciseCommand")
    .post("/v1/create-and-exercise")
    .body(StringBody(jsonCommand))

  private val scn = scenario("CreateAndExerciseCommandScenario")
    .repeat(numberOfRuns / defaultNumUsers)(exec(request.silent)) // server warmup
    .repeat(numberOfRuns / defaultNumUsers)(exec(request))

  setUp(
    scn.inject(atOnceUsers(defaultNumUsers))
  ).protocols(httpProtocol)
}
