// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class CreateCommand extends Simulation {

  private val httpProtocol = http.disableWarmUp
    .baseUrl("http://localhost:7575")
    .inferHtmlResources()
    .acceptHeader("*/*")
    .acceptEncodingHeader("gzip, deflate")
    .authorizationHeader(
      "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJmb29iYXIiLCJhY3RBcyI6WyJBbGljZSJdfX0.VdDI96mw5hrfM5ZNxLyetSVwcD7XtLT4dIdHIOa9lcU")
    .contentTypeHeader("application/json")

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
    .repeat(2000)(exec(request.silent)) // server warmup
    .repeat(2000)(exec(request))

  setUp(
    scn.inject(atOnceUsers(1))
  ).protocols(httpProtocol)
}
