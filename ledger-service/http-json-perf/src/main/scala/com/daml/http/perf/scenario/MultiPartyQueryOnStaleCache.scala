// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.core.structure.PopulationBuilder

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class MultiPartyQueryOnStaleCache extends Simulation with SimulationConfig with HasRandomAmount {

  private val createRequest =
    http("CreateCommand")
      .post("/v1/create")
      .body(StringBody(s"""{
  "templateId": "Iou:Iou",
  "payload": {
    "issuer": "$aliceParty",
    "owner": "$aliceParty",
    "currency": "USD",
    "amount": "1",
    "observers": ["$bobParty", "$charlieParty"]
  }
}"""))

  private def queryRequest(parties: Seq[String]) = {
    val readAs = parties.map("\"" + _ + "\"").mkString("[", ",", "]")
    val body = s"""{
    "templateIds": ["Iou:Iou"],
    "query": {"amount": "0"},
    "readers": ${readAs}
}"""
    http("QueryRequest")
      .post("/v1/query")
      .body(StringBody(body))
  }

  private def createContracts(step: String) =
    scenario(s"MultiPartyQueryOnStaleCache - CreateContracts - $step")
      .exec(createRequest.silent)

  private def queryBy(parties: String*) =
    scenario(s"MultiPartyQueryOnStaleCache - Query By ${parties.mkString(",")}")
      .exec(queryRequest(parties))

  private def inOrder(steps: Seq[PopulationBuilder]): PopulationBuilder = steps match {
    case head :: Nil => head
    case head :: tail => head.andThen(inOrder(tail))
  }

  setUp(
    inOrder(
      Seq(
        createContracts("initialize").inject(atOnceUsers(50)),
        queryBy(aliceParty).inject(atOnceUsers(1)),
        queryBy(bobParty).inject(atOnceUsers(1)),
        queryBy(charlieParty).inject(atOnceUsers(1)),
        createContracts("update").inject(atOnceUsers(1)),
        queryBy(aliceParty, bobParty, charlieParty).inject(atOnceUsers(20)),
      )
    )
  ).protocols(httpProtocolForBearer(aliceBobCharlieJwt))
}
