// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryConstantAcs extends Simulation with SimulationConfig with HasRandomAmount {

  private val createRequest =
    http("CreateCommand")
      .post("/v1/create")
      .body(StringBody(s"""{
  "templateId": "Iou:Iou",
  "payload": {
    "issuer": "$aliceParty",
    "owner": "$aliceParty",
    "currency": "USD",
    "amount": "$${amount}",
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
    .repeat(5000 / defaultNumUsers) {
      // populate the ACS
      feed(Iterator.continually(Map("amount" -> randomAmount())))
        .exec(createRequest.silent)
    }
    .repeat(500 / defaultNumUsers) {
      // run queries
      feed(Iterator.continually(Map("amount" -> randomAmount())))
        .exec(queryRequest)
    }

  setUp(
    scn.inject(atOnceUsers(defaultNumUsers))
  ).protocols(httpProtocol)
}
