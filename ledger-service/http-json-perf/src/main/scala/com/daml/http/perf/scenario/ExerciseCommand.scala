// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ExerciseCommand extends Simulation with SimulationConfig {

  private val createCommand = """{
  "templateId": "Iou:Iou",
  "payload": {
    "issuer": "Alice",
    "owner": "Alice",
    "currency": "USD",
    "amount": "9.99",
    "observers": []
  }
}"""

  private val exerciseCommand = """{
    "templateId": "Iou:Iou",
    "contractId": "${contractId}",
    "choice": "Iou_Transfer",
    "argument": {
        "newOwner": "Alice"
    }
  }"""

  private val createRequest = http("CreateCommand")
    .post("/v1/create")
    .body(StringBody(createCommand))

  private val exerciseRequest =
    http("ExerciseCommand")
      .post("/v1/exercise")
      .body(StringBody(exerciseCommand))

  private val scn = scenario("ExerciseCommandScenario")
    .repeat(2000)(exec(createRequest.silent)) // populate the ACS
    .exec(
      // retrieve all contractIds
      http("GetACS")
        .get("/v1/query")
        .check(status.is(200), jsonPath("$.result[*].contractId").findAll.saveAs("contractIds"))
        .silent
    )
    .foreach("${contractIds}", "contractId")(exec(exerciseRequest))

  setUp(
    scn.inject(atOnceUsers(1))
  ).protocols(httpProtocol)
}
