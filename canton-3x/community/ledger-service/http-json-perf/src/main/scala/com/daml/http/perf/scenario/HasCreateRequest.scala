// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._
import io.gatling.http.check.HttpCheck
import io.gatling.http.request.builder.HttpRequestBuilder

private[scenario] trait HasCreateRequest {
  this: HasRandomAmount with SimulationConfig =>

  private lazy val acsQueue: BlockingQueue[String] = new LinkedBlockingQueue[String]()

  def acsSize(): Int = acsQueue.size

  // does not block throws an exception if queue is empty
  def removeNextContractIdFromAcs(): String = acsQueue.remove

  lazy val randomAmountCreateRequest: HttpRequestBuilder =
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

  lazy val captureContractId: HttpCheck =
    jsonPath("$.result.contractId").transform(x => acsQueue.put(x))

  def fillAcsScenario(size: Int, silent: Boolean): ScenarioBuilder =
    scenario(s"FillAcsScenario, size: $size")
      .repeat(size / defaultNumUsers) {
        feed(Iterator.continually(Map("amount" -> randomAmount())))
          .group("FillAcsGroup") {
            val create =
              if (silent) randomAmountCreateRequest.silent else randomAmountCreateRequest.notSilent
            exec(create.check(status.is(200)).check(captureContractId)).exitHereIfFailed
          }
      }
}
