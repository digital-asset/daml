// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryVariableAcs
    extends Simulation
    with SimulationConfig
    with HasRandomAmount
    with HasCreateRequest
    with HasQueryRequest
    with HasArchiveRequest {

  private val wantedAcsSize = 5000

  private val numberOfRuns = 500

  private val syncQueryScenario =
    scenario(s"SyncQueryWithVariableAcsScenario, numberOfRuns: $numberOfRuns")
      .doWhile(_ => acsSize() < wantedAcsSize) {
        pause(1.second)
      }
      .repeat(numberOfRuns) {
        feed(
          Iterator.continually(
            Map[String, String](
              "amount" -> String.valueOf(randomAmount()),
              "archiveContractId" -> removeNextContractIdFromAcs(),
            )
          )
        ).exec {
          // run query in parallel with archive and create
          randomAmountQueryRequest.notSilent.resources(
            archiveRequest.silent,
            randomAmountCreateRequest.silent,
          )
        }
      }

  setUp(
    fillAcsScenario(wantedAcsSize, silent = true).inject(atOnceUsers(defaultNumUsers)).andThen {
      syncQueryScenario.inject(atOnceUsers(1))
    }
  ).protocols(httpProtocol)
}
