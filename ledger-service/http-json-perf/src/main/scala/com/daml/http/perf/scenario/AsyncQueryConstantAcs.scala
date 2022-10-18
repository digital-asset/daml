// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.http.check.ws.WsFrameCheck

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class AsyncQueryConstantAcs
    extends Simulation
    with SimulationConfig
    with HasRandomAmount
    with HasCreateRequest {

  private val wantedAcsSize = 5000

  private val waitForResponse: FiniteDuration = 5.seconds

  private val numberOfRuns = 100

  private val queryRequest =
    """{"templateIds": ["Iou:Iou"], "query": {"amount": {"%gt": 1.0}}}"""

  val messageCheck: WsFrameCheck.Text = ws
    .checkTextMessage("messageCheck")
    .check(jsonPath("$.offset").find.notExists)
    .check(jsonPath("$.events[*].created").findAll)

  private def query(runId: Int) = {
    val wsName = s"websocket$runId"
    ws("Connect websocket", wsName)
      .connect("/v1/stream/query")
      .subprotocol(s"jwt.token.$aliceJwt, daml.ws.auth")
      .onConnected(
        exec(
          ws(s"Send Query Request and wait for response: $waitForResponse", wsName)
            .sendText(queryRequest)
            .await(waitForResponse)(Vector.fill(5)(messageCheck): _*)
        )
      )
  }

  private val asyncQueryScenario = scenario(s"AsyncQueryConstantAcs, numberOfRuns: $numberOfRuns")
    .doWhile(_ => acsSize() < wantedAcsSize) {
      pause(1.second)
    }
    .exec(
      1.to(numberOfRuns / defaultNumUsers).map(runId => exec(query(runId)))
    )

  setUp(
    fillAcsScenario(wantedAcsSize, silent = true).inject(atOnceUsers(defaultNumUsers)).andThen {
      asyncQueryScenario.inject(atOnceUsers(defaultNumUsers))
    }
  ).protocols(httpProtocol)

}
