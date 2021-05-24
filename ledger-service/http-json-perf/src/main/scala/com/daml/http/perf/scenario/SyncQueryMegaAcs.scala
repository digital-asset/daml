// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import spray.json._, DefaultJsonProtocol._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryMegaAcs extends Simulation with SimulationConfig with HasRandomAmount {
  import SyncQueryMegaAcs._

  private[this] val notAliceParty = "NotAlice"
  private[this] val notAliceJwt = aliceJwt // TODO SC not

  private type Discriminator = (String, String, Seq[Seq[String]])

  private[this] val discriminators: Seq[Discriminator] = Seq(
    // label, readAs, observers
    (
      "party matches by observer distribution",
      Seq(notAliceJwt),
      Seq(
        Seq((Seq(notAliceParty), 1), (Seq.empty, 99)),
        Seq((Seq(notAliceParty), 1), (Seq.empty, 19)),
        Seq((Seq(notAliceParty), 1), (Seq.empty, 9)),
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
    "totalSteps": 1000,
    "amountCycle": [${amount}],
    "observersCycle": ${observersCycle}
  }
}"""))

  private val queryRequest =
    http("SyncQueryRequest")
      .post("/v1/query")
      .body(StringBody("""{
    "templateIds": ["LargeAcs:Iou"],
    "query": {"amount": ${amount}}
}"""))

  private val scns = discriminators map { case (scnName, jwtTODO @ _, observersCycle) =>
    val env = Map("observersCycle" -> observersCycle.toJson)
    scenario(s"SyncQueryMegaScenario $scnName")
      .exec(createRequest.silent)
      // populate the ACS
      .repeat(100, "amount") {
        feed(Iterator continually env)
          .exec(createManyRequest.silent)
      }
      // run queries
      .repeat(500) {
        feed(Iterator.continually(Map("amount" -> randomAmount())))
          .exec(queryRequest)
      }
  }

  private val scn = scns.head

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
