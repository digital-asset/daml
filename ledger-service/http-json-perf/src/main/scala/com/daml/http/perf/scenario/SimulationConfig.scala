// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.http.protocol.HttpProtocolBuilder

private[scenario] trait SimulationConfig {
  lazy val httpProtocol: HttpProtocolBuilder = http
    .baseUrl(s"http://$hostAndPort")
    .wsBaseUrl(s"ws://$hostAndPort")
    .warmUp(s"http://$hostAndPort/v1/query")
    .inferHtmlResources()
    .acceptHeader("*/*")
    .acceptEncodingHeader("gzip, deflate")
    .authorizationHeader(s"Bearer $aliceJwt")
    .contentTypeHeader("application/json")

  private val hostAndPort = "localhost:7575"

  // {"https://daml.com/ledger-api": {"ledgerId": "MyLedger", "applicationId": "foobar", "actAs": ["Alice"]}}
  val aliceJwt: String =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJmb29iYXIiLCJhY3RBcyI6WyJBbGljZSJdfX0.VdDI96mw5hrfM5ZNxLyetSVwcD7XtLT4dIdHIOa9lcU"

}
