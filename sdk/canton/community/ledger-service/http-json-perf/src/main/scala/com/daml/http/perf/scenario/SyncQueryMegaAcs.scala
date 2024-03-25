// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.core.feeder.Record
import io.gatling.http.Predef._
import spray.json._, DefaultJsonProtocol._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SyncQueryMegaAcs extends Simulation with SimulationConfig with HasRandomAmount {
  import SyncQueryMegaAcs._

  private type Discriminator = (String, String, Seq[Seq[String]], Seq[WhichTemplate])

  private[this] val discriminators: Seq[Discriminator] = Seq(
    // label, readAs, observers
    (
      "party matches by observer distribution",
      Seq(bobJwt),
      Seq(
        Seq((Seq(bobParty), 1), (Seq.empty, 99)),
        Seq((Seq(bobParty), 1), (Seq.empty, 19)),
        Seq((Seq(bobParty), 1), (Seq.empty, 9)),
      ),
      Seq(Seq(UseIou -> 1)),
    ),
    (
      "all matches by template ID distribution",
      Seq(bobJwt),
      Seq(Seq(Seq.empty -> 1), Seq(Seq(bobParty) -> 1, Seq.empty -> 99)),
      Seq(Seq(UseIou -> 1, UseNotIou -> 99)),
    ),
  ).flatMap { case (label, readAses, observerses, whichTemplates) =>
    for {
      readAs <- readAses
      observers <- observerses
      whichTemplate <- whichTemplates
    } yield (
      s"$label, party ${reportCycle(observers) * 100}% positive + tpid ${reportCycle(whichTemplate) * 100}% positive",
      readAs,
      makeCycle(observers),
      makeCycle(whichTemplate),
    )
  }

  private val createRequest =
    http("CreateCommand")
      .post("/v1/create")
      .body(StringBody(s"""{
  "templateId": "LargeAcs:Genesis",
  "payload": {
    "issuer": "$aliceParty",
    "owner": "$aliceParty",
    "currency": "USD",
    "observers": []
  }
}"""))

  private val createManyRequest =
    http("ExerciseCommand")
      .post("/v1/exercise")
      .body(StringBody(s"""{
  "templateId": "LargeAcs:Genesis",
  "key": "$aliceParty",
  "choice": "Genesis_MakeIouRange",
  "argument": {
    "totalSteps": 10000,
    "amountCycle": [$${amount}],
    "observersCycle": $${observersCycle},
    "whichTemplateCycle": $${whichTemplateCycle}
  }
}"""))

  private val queryRequest =
    http("SyncQueryRequest")
      .post("/v1/query")
      .header(HttpHeaderNames.Authorization, "Bearer ${reqJwt}")
      .body(StringBody("""{
    "templateIds": ["LargeAcs:${templateId}"],
    "query": {"amount": ${amount}}
}"""))

  private val scns = discriminators map {
    case (scnName, reqJwt, observersCycle, whichTemplateCycle) =>
      val env = Map(
        "observersCycle" -> observersCycle.toJson,
        "whichTemplateCycle" -> whichTemplateCycle.map(_.productPrefix).toJson,
      )
      scenario(s"SyncQueryMegaScenario $scnName")
        .exec(createRequest.silent)
        // populate the ACS
        .repeat(10 / defaultNumUsers, "amount") {
          feed(Iterator continually env)
            .exec(createManyRequest.silent)
        }
        // run queries
        .repeat(500 / defaultNumUsers) {
          // unless we request under Alice, we don't get negatives in the DB
          def m(amount: Int, reqJwt: String, templateId: String): Record[Any] =
            Map("amount" -> amount, "reqJwt" -> reqJwt, "templateId" -> templateId)
          feed(
            (for (templateId <- Seq("Iou", "NotIou"))
              yield m(randomAmount(), aliceJwt, templateId)).iterator
              ++ Iterator.continually(m(randomAmount(), reqJwt, "Iou"))
          )
            .exec(queryRequest)
        }
  }

  // TODO SC run other scns
  private val scn = scns.head

  setUp(
    scn.inject(atOnceUsers(defaultNumUsers))
  ).protocols(httpProtocol)
}

object SyncQueryMegaAcs {
  private sealed abstract class WhichTemplate extends Product with Serializable
  private case object UseIou extends WhichTemplate
  private case object UseNotIou extends WhichTemplate

  private def reportCycle(s: Seq[(_, Int)]): Double = {
    val (_, h) +: _ = s
    h.toDouble / s.view.map(_._2).sum
  }

  private def makeCycle[A](s: Seq[(A, Int)]): Seq[A] =
    s flatMap { case (a, n) => Iterator.fill(n)(a) }
}
