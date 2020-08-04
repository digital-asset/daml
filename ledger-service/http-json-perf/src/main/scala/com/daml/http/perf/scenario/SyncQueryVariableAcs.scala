// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import java.{util => jutil}

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryVariableAcs extends Simulation with SimulationConfig with HasRandomAmount {

  private val acsQueue = new jutil.concurrent.LinkedBlockingQueue[String]()

  private val createRequest =
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

  private val queryRequest =
    http("SyncQueryRequest")
      .post("/v1/query")
      .body(StringBody("""{
    "templateIds": ["Iou:Iou"],
    "query": {"amount": ${amount}}
}"""))

  private val exerciseRequest =
    http("ExerciseCommand")
      .post("/v1/exercise")
      .body(StringBody("""{
    "templateId": "Iou:Iou",
    "contractId": "${contractId}",
    "choice": "Iou_Transfer",
    "argument": {
        "newOwner": "Alice"
    }
  }"""))

  private val createContractScn = scenario("CreateContractScenario")
    .repeat(1000) {
      feed(Iterator.continually(Map("amount" -> randomAmount()))).exec(createRequest)
    }

  private val exerciseTransferScn = scenario("ExerciseTransferScenario")
    .repeat(1000) {
      feed(BlockingIterator(acsQueue, 1000).map(x => Map("contractId" -> x))).exec(exerciseRequest)
    }

  private val syncQueryScn = scenario("SyncQueryScenario")
    .repeat(500) {
      feed(Iterator.continually(Map("amount" -> randomAmount()))).exec(queryRequest)
    }

  setUp(
    createContractScn.inject(atOnceUsers(1)),
    syncQueryScn.inject(atOnceUsers(1)),
    exerciseTransferScn.inject(atOnceUsers(1)),
  ).protocols(httpProtocol)
}

private[scenario] final case class BlockingIterator[A](
    private val queue: jutil.concurrent.BlockingQueue[A],
    private val maxToRetrieve: Int)
    extends Iterator[A] {

  private val retrieved = new jutil.concurrent.atomic.AtomicInteger(0)

  override def hasNext: Boolean = retrieved.get < maxToRetrieve

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def next(): A = {
    val a: A = queue.take() // this blocks waiting for the next element in the queue if it is empty
    retrieved.incrementAndGet()
    a
  }
}
