// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.http.check.HttpCheck

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

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
      .repeat(numberOfRuns / defaultNumUsers) {
        val acsQueue: BlockingQueue[String] = new LinkedBlockingQueue[String]()
        val captureContractId: HttpCheck =
          jsonPath("$.result.contractId").transform(x => acsQueue.put(x))
        group("Populate ACS") {
          doWhile(_ => acsQueue.size() < wantedAcsSize) {
            feed(Iterator.continually(Map("amount" -> String.valueOf(randomAmount()))))
              .exec {
                randomAmountCreateRequest.check(status.is(200), captureContractId).silent
              }
          }
        }.group("Run Query") {
          feed(Iterator.continually(Map("amount" -> String.valueOf(randomAmount()))))
            .exec(randomAmountQueryRequest.notSilent)
        }.group("Archive ACS") {
          doWhile(_ => acsQueue.size() > 0) {
            feed(Iterator.continually(Map("archiveContractId" -> acsQueue.poll())))
              .exec(archiveRequest.silent)
          }
        }
      }

  setUp(
    syncQueryNewAcs.inject(atOnceUsers(defaultNumUsers))
  ).protocols(httpProtocol)
}
