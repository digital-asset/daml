// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import java.{util => jutil}

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryVariableAcs extends Simulation with SimulationConfig {

  private val rng = new scala.util.Random(123456789)

  // called from two different scenarios, need to synchronize access
  private def randomAmount(): Int = {
    val x = this.synchronized { rng.nextInt(10) }
    x + 5 // [5, 15)
  }

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
        jsonPath("$.result[*].contractId").transform(x => acsQueue.put(x))
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
    .repeat(50) {
      feed(Iterator.continually(Map("amount" -> randomAmount())))
        .exec(createRequest.silent)
    }

  private val exerciseTransferScn = scenario("ExerciseTransferScenario")
    .repeat(5) {
      feed(BlockingIterator(acsQueue, 50).map(x => Map("contractId" -> x)))
        .foreach("${contractIds}", "contractId")(exec(exerciseRequest))
    }

  private val syncQueryScn = scenario("SyncQueryScenario")
    .repeat(5) {
      feed(Iterator.continually(Map("amount" -> randomAmount())))
        .exec(queryRequest)
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

  override def isEmpty: Boolean = synchronized {
    queue.size == 0 || retrieved.get >= maxToRetrieve
  }

  override def hasNext: Boolean = !isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def next(): A = synchronized {
    val a: A = queue.take()
    retrieved.incrementAndGet()
    a
  }
}
