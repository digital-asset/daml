// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryNewAcs
    extends Simulation
    with SimulationConfig
    with HasRandomAmount
    with HasCreateRequest
    with HasArchiveRequest
    with HasQueryRequest {

  private val wantedAcsSize = 1000

  private val numberOfRuns = 25

  private val syncQueryNewAcs =
    scenario(s"SyncQueryNewAcs, numberOfRuns: $numberOfRuns, ACS size: $wantedAcsSize")
      .repeat(numberOfRuns) {
        group("Populate ACS") {
          doWhile(_ => this.acsSize() < wantedAcsSize) {
            feed(Iterator.continually(Map("amount" -> String.valueOf(randomAmount()))))
              .exec(
                randomAmountCreateRequest
                  .check(status.is(200))
                  .check(captureContractId)
                  .silent)
          }
        }.group("Run Query") {
            feed(Iterator.continually(Map("amount" -> String.valueOf(randomAmount()))))
              .exec(randomAmountQueryRequest.notSilent)
          }
          .group("Archive ACS") {
            doWhile(_ => this.acsSize() > 0) {
              feed(Iterator.continually(Map("archiveContractId" -> takeNextContractIdFromAcs())))
                .exec(archiveRequest.silent)
            }
          }
      }

  setUp(
    syncQueryNewAcs.inject(atOnceUsers(1)),
  ).protocols(httpProtocol)
}
