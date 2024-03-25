// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.http.protocol.HttpProtocolBuilder

private[scenario] trait SimulationConfig {
  import SimulationConfig._

  private def getPropertyOrFail(name: String): String =
    Option(System.getProperty(name))
      .getOrElse(throw new RuntimeException("Missing system property " + name))

  lazy val httpProtocol: HttpProtocolBuilder = http
    .baseUrl(s"http://$hostAndPort")
    .wsBaseUrl(s"ws://$hostAndPort")
    .warmUp(s"http://$hostAndPort/v1/query")
    .inferHtmlResources()
    .acceptHeader("*/*")
    .acceptEncodingHeader("gzip, deflate")
    .authorizationHeader(s"Bearer $aliceJwt")
    .contentTypeHeader("application/json")

  protected[this] val defaultNumUsers = 10
  private lazy val hostAndPort: String = System.getProperty(HostAndPortKey, "localhost:7575")

  protected[this] lazy val aliceParty: String = getPropertyOrFail(AlicePartyKey)
  protected[this] lazy val aliceJwt: String = getPropertyOrFail(AliceJwtKey)

  protected[this] lazy val bobParty: String = getPropertyOrFail(BobPartyKey)
  protected[this] lazy val bobJwt: String = getPropertyOrFail(BobJwtKey)

  protected[this] lazy val charlieParty: String = getPropertyOrFail(CharliePartyKey)
  protected[this] lazy val charlieJwt: String = getPropertyOrFail(CharlieJwtKey)
}

object SimulationConfig {
  val HostAndPortKey = "com.daml.http.perf.hostAndPort"
  // Parties
  val AlicePartyKey = "com.daml.http.perf.aliceParty"
  val AliceJwtKey = "com.daml.http.perf.aliceJwt"
  val BobPartyKey = "com.daml.http.perf.bobParty"
  val BobJwtKey = "com.daml.http.perf.bobJwt"
  val CharliePartyKey = "com.daml.http.perf.charlieParty"
  val CharlieJwtKey = "com.daml.http.perf.charlieJwt"
}
