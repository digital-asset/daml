// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedCreatedEvent
import com.digitalasset.canton.console.ParticipantReferenceCommon
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.topology.PartyId

/** Adds the ability to run cycles to integration tests
  *
  * Please note that you need to upload CantonExamples.dar to the ledger before you can use this test
  */
trait HasCycleUtils[E <: Environment, TCE <: TestConsoleEnvironment[E]] {
  this: BaseIntegrationTest[E, TCE] =>

  /** @param partyId assumes that the party is hosted on participant1 AND participant2 (in the simplest case
    * this could simply mean that participant1 == participant2)
    */
  def runCycle(
      partyId: PartyId,
      participant1: ParticipantReferenceCommon,
      participant2: ParticipantReferenceCommon,
      commandId: String = "",
  ): Unit = {

    Seq(participant2, participant1).map { participant =>
      if (participant.packages.find("Cycle").isEmpty) {
        participant.dars.upload(CantonExamplesPath)
      }
    }

    def p2acs(): Seq[LedgerApiTypeWrappers.WrappedCreatedEvent] =
      participant2.ledger_api_v2.state.acs
        .of_party(partyId)
        .filter(_.templateId.isModuleEntity("Cycle", "Cycle"))
        .map(entry => WrappedCreatedEvent(entry.event))

    p2acs() shouldBe empty

    clue("creating cycle " + commandId) {
      createCycleContract(participant1, partyId, "I SHALL CREATE", commandId)
    }
    val coid = participant2.ledger_api_v2.javaapi.state.acs.await(M.Cycle.COMPANION)(partyId)
    val cycleEx = coid.id.exerciseArchive().commands.loneElement
    clue("submitting response") {
      participant2.ledger_api_v2.javaapi.commands.submit(
        Seq(partyId),
        Seq(cycleEx),
        commandId = (if (commandId.isEmpty) "" else s"$commandId-response"),
      )
    }
    eventually() {
      p2acs() shouldBe empty
    }
  }

  def createCycleContract(
      participant: ParticipantReferenceCommon,
      partyId: PartyId,
      id: String,
      commandId: String = "",
  ): Unit = {
    if (participant.packages.find("Cycle").isEmpty) {
      participant.dars.upload(CantonExamplesPath)
    }
    val cycle = new M.Cycle(id, partyId.toProtoPrimitive).create.commands.loneElement
    participant.ledger_api_v2.javaapi.commands
      .submit(Seq(partyId), Seq(cycle), commandId = commandId)

  }
}
