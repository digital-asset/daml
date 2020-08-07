// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryNewAcs
    extends Simulation
    with SimulationConfig
    with HasRandomAmount
    with HasCreateRequest
    with HasArchiveRequest
    with HasQueryRequest {

  private val wantedAcsSize = 1000

  private val numberOfRuns = 100

  private val syncQueryNewAcs =
    scenario(s"SyncQueryNewAcs, numberOfRuns: $numberOfRuns")
      .doWhile(_ => acsSize() < wantedAcsSize) {
        pause(1.second)
      }
      .repeat(numberOfRuns) {
        // run a query
        feed(Iterator.continually(Map("amount" -> String.valueOf(randomAmount()))))
          .exec(randomAmountQueryRequest.notSilent)
          // archive and re-fill the ACS
          .repeat(wantedAcsSize)(
            feed(
              Iterator.continually(
                Map[String, String](
                  "archiveContractId" -> takeNextContractIdFromAcs(),
                  "amount" -> String.valueOf(randomAmount())
                )
              )
            ).exec(archiveRequest.silent)
              .exec(
                randomAmountCreateRequest
                  .check(status.is(200))
                  .check(captureContractId)
                  .silent)
          )
      }

  setUp(
    fillAcsScenario(wantedAcsSize, silent = true).inject(atOnceUsers(1)),
    syncQueryNewAcs.inject(atOnceUsers(1)),
  ).protocols(httpProtocol)
}
