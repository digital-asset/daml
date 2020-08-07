// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryVariableAcs
    extends Simulation
    with SimulationConfig
    with HasRandomAmount
    with HasCreateRequest {

  private val queryRequest =
    http("SyncQueryRequest")
      .post("/v1/query")
      .body(StringBody("""{
    "templateIds": ["Iou:Iou"],
    "query": {"amount": ${amount}}
}"""))

  private val archiveRequest =
    http("ExerciseCommand")
      .post("/v1/exercise")
      .body(StringBody("""{
    "templateId": "Iou:Iou",
    "contractId": "${archiveContractId}",
    "choice": "Archive",
    "argument": {}
  }"""))

  private val wantedAcsSize = 5000

  private val numberOfRuns = 500

  private val syncQueryScn =
    scenario(s"SyncQueryWithVariableAcsScenario, numberOfRuns: $numberOfRuns")
      .doWhile(_ => acsQueue.size() < wantedAcsSize) {
        pause(1.second)
      }
      .repeat(numberOfRuns) {
        feed(
          Iterator.continually(
            Map[String, String](
              "amount" -> String.valueOf(randomAmount()),
              "archiveContractId" -> acsQueue.take(),
            )
          )
        ).exec {
          // run query in parallel with archive and create
          queryRequest.notSilent.resources(
            archiveRequest.silent,
            createRequestAndCollectContractId.silent
          )
        }
      }

  setUp(
    fillAcsScenario(wantedAcsSize).inject(atOnceUsers(1)),
    syncQueryScn.inject(atOnceUsers(1)),
  ).protocols(httpProtocol)
}
