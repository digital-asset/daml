// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryConstantAcs extends Simulation with SimulationConfig with HasRandomAmount {

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

  private val queryRequest =
    http("SyncQueryRequest")
      .post("/v1/query")
      .body(StringBody("""{
    "templateIds": ["Iou:Iou"],
    "query": {"amount": ${amount}}
}"""))

  private val scn = scenario("SyncQueryScenario")
    .repeat(5000) {
      // populate the ACS
      feed(Iterator.continually(Map("amount" -> randomAmount())))
        .exec(createRequest.silent)
    }
    .repeat(500) {
      // run queries
      feed(Iterator.continually(Map("amount" -> randomAmount())))
        .exec(queryRequest)
    }

  setUp(
    scn.inject(atOnceUsers(1))
  ).protocols(httpProtocol)
}
