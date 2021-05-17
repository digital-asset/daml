// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryMegaAcs extends Simulation with SimulationConfig with HasRandomAmount {
  import SyncQueryMegaAcs._

  private[this] val notAliceJwt = aliceJwt // TODO SC ... not

  /* TODO SC private[this] */
  val discriminators = Seq(
    // label, readAs, observers
    (
      "party matches by observer distribution",
      Seq(notAliceJwt),
      Seq(
        Seq((Seq(notAliceJwt), 1), (Seq.empty, 99)),
        Seq((Seq(notAliceJwt), 1), (Seq.empty, 19)),
        Seq((Seq(notAliceJwt), 1), (Seq.empty, 9)),
      ),
    ),
    ("", Seq(), Seq()),
  ).flatMap { case (label, readAses, observerses) =>
    for {
      readAs <- readAses
      observers <- observerses
    } yield (s"$label, ${reportCycle(observers) * 100}% positive", readAs, makeCycle(observers))
  }

  private val createRequest =
    http("CreateCommand")
      .post("/v1/create")
      .body(StringBody("""{
  "templateId": "LargeAcs:Genesis",
  "payload": {
    "issuer": "Alice",
    "owner": "Alice",
    "currency": "USD",
    "observers": []
  }
}"""))

  private val createManyRequest =
    http("ExerciseCommand")
      .post("/v1/exercise")
      .body(StringBody("""{
  "templateId": "LargeAcs:Genesis",
  "key": "Alice",
  "choice": "Genesis_MakeIouRange",
  "argument": {
    "totalSteps": 100,
    "amountCycle": [${amount}],
    "observersCycle": [[]]
  }
}"""))

  private val queryRequest =
    http("SyncQueryRequest")
      .post("/v1/query")
      .body(StringBody("""{
    "templateIds": ["LargeAcs:Iou"],
    "query": {"amount": ${amount}}
}"""))

  private val scn = scenario("SyncQueryMegaScenario")
    .exec(createRequest.silent)
    .repeat(50, "amount") {
      // populate the ACS
      exec(createManyRequest.silent)
    }
    .repeat(500) {
      // run queries
      feed(Iterator.continually(Map("amount" -> randomAmount())))
        .exec(queryRequest)
    }

  setUp(
    scn.inject(atOnceUsers(1))
  ).protocols(httpProtocol)
}

object SyncQueryMegaAcs {
  private def reportCycle(s: Seq[(_, Int)]): Double = {
    val (_, h) +: _ = s
    h.toDouble / s.view.map(_._2).sum
  }

  private def makeCycle[A](s: Seq[(A, Int)]): Seq[A] =
    s flatMap { case (a, n) => Iterator.fill(n)(a) }
}
