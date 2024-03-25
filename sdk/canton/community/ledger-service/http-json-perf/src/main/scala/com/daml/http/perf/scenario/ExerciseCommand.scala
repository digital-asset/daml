// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ExerciseCommand extends Simulation with SimulationConfig {

  private val createCommand = s"""{
  "templateId": "Iou:Iou",
  "payload": {
    "issuer": "$aliceParty",
    "owner": "$aliceParty",
    "currency": "USD",
    "amount": "9.99",
    "observers": []
  }
}"""

  private val exerciseCommand = s"""{
    "templateId": "Iou:Iou",
    "contractId": "$${contractId}",
    "choice": "Iou_Transfer",
    "argument": {
        "newOwner": "$aliceParty"
    }
  }"""

  private val numberOfRuns = 2000
  private val createRequest = http("CreateCommand")
    .post("/v1/create")
    .body(StringBody(createCommand))

  private val exerciseRequest =
    http("ExerciseCommand")
      .post("/v1/exercise")
      .body(StringBody(exerciseCommand))

  private val scn = scenario("ExerciseCommandScenario-Create")
    .repeat(numberOfRuns / defaultNumUsers)(exec(createRequest.silent)) // populate the ACS

  private val queryScn = scenario("ExerciseCommandScenario-QueryAndExercise")
    .repeat(1)(
      exec(
        // retrieve all contractIds
        http("GetACS")
          .get("/v1/query")
          .check(status.is(200), jsonPath("$.result[*].contractId").findAll.saveAs("contractIds"))
          .silent
      )
    )
    .foreach("${contractIds}", "contractId")(exec(exerciseRequest))

  setUp(
    scn
      .inject(atOnceUsers(defaultNumUsers))
      .andThen(queryScn.inject(atOnceUsers(1)))
  ).protocols(httpProtocol)
}
