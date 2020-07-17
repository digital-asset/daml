// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryConstantAcs extends Simulation {

  private val rng = new scala.util.Random(123456789)

  private def randomAmount(): Int = {
    rng.nextInt(10) + 5 // [5, 15)
  }

  private val httpProtocol = http.disableWarmUp
    .baseUrl("http://localhost:7575")
    .inferHtmlResources()
    .acceptHeader("*/*")
    .acceptEncodingHeader("gzip, deflate")
    .authorizationHeader(
      "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJmb29iYXIiLCJhY3RBcyI6WyJBbGljZSJdfX0.VdDI96mw5hrfM5ZNxLyetSVwcD7XtLT4dIdHIOa9lcU")
    .contentTypeHeader("application/json")

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
