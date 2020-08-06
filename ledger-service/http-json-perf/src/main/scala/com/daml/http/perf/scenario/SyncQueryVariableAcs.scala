// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import java.{util => jutil}

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryVariableAcs extends Simulation with SimulationConfig with HasRandomAmount {

  private val acsQueue = new jutil.concurrent.LinkedBlockingQueue[String]()

  private val createRequest =
    http("CreateCommand")
      .post("/v1/create")
      .body(StringBody("""{
  "templateId": "Iou:Iou",
  "payload": {
    "issuer": "Alice",
    "owner": "Alice",
    "currency": "USD",
    "amount": "${amount}",
    "observers": []
  }
}"""))
      .check(
        status.is(200),
        jsonPath("$.result.contractId").transform(x => acsQueue.put(x))
      )

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

  private val fillAcs = scenario(s"FillAcsScenario, size: $wantedAcsSize")
    .doWhile(_ => acsQueue.size() < wantedAcsSize) {
      feed(Iterator.continually(Map("amount" -> randomAmount()))).exec(createRequest.silent)
    }

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
            createRequest.silent
          )
        }
      }

  setUp(
    fillAcs.inject(atOnceUsers(1)),
    syncQueryScn.inject(atOnceUsers(1)),
  ).protocols(httpProtocol)
}
