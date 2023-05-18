// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.{PopulationBuilder, StructureSupport}
import io.gatling.http.Predef._
import scalaz._

private[scenario] trait HasPartyRequest extends StructureSupport{

  private lazy val aliceParty =
    """{
      |"identifierHint": "Alice",
      |"displayName": "Alice & Co. LLC"
      |}""".stripMargin

  private lazy val bobParty =
    """{
      |"identifierHint": "Bob"
      |}""".stripMargin

  private lazy val trentParty =
    """{
      |"identifierHint": "Trent"
      |}""".stripMargin

  protected lazy val alice = http("CreateParty")
    .post("/v1/parties/allocate")
    .body(StringBody(aliceParty))

  protected lazy val bob = http("CreateParty")
    .post("/v1/parties/allocate")
    .body(StringBody(bobParty))

  protected lazy val trent= http("CreateParty")
    .post("/v1/parties/allocate")
    .body(StringBody(trentParty))

  def withParties(parties: NonEmptyList[ActionBuilder])(nextStep: PopulationBuilder): PopulationBuilder =
      scenario("CreateParties")
        .repeat(1)(chain(parties.list.toList))
        .inject(atOnceUsers(1))
        .andThen(nextStep)
}
