// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._
import io.gatling.http.request.builder.HttpRequestBuilder

private[scenario] trait HasCreateRequest {
  this: HasRandomAmount =>

  lazy val acsQueue: BlockingQueue[String] = new LinkedBlockingQueue[String]()

  lazy val createRequestAndCollectContractId: HttpRequestBuilder =
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
      .check(
        status.is(200),
        jsonPath("$.result.contractId").transform(x => acsQueue.put(x))
      )

  def fillAcsScenario(size: Int): ScenarioBuilder =
    scenario(s"FillAcsScenario, size: $size")
      .doWhile(_ => acsQueue.size() < size) {
        feed(Iterator.continually(Map("amount" -> randomAmount())))
          .exec(createRequestAndCollectContractId.silent)
      }
}
