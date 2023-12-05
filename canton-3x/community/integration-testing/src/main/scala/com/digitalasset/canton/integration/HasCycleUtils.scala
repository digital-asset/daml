// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers
import com.digitalasset.canton.console.ParticipantReferenceCommon
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.participant.admin.workflows.java.pingpong as M
import com.digitalasset.canton.topology.PartyId

/** Adds the ability to run pingpong cycles to integration tests */
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

    def p2acs(): Seq[LedgerApiTypeWrappers.WrappedCreatedEvent] =
      participant2.ledger_api.acs
        .of_party(partyId)
        .filter(_.templateId.isModuleEntity("PingPong", "Cycle"))

    p2acs() shouldBe empty

    createCycleContract(participant1, partyId, "I SHALL CREATE", commandId)
    val coid = participant2.ledger_api.javaapi.acs.await(M.Cycle.COMPANION)(partyId)
    val cycleEx = coid.id.exerciseVoid().commands.loneElement
    participant2.ledger_api.javaapi.commands.submit(
      Seq(partyId),
      Seq(cycleEx),
      commandId = (if (commandId.isEmpty) "" else s"$commandId-response"),
    )
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

    val cycle = new M.Cycle(id, partyId.toProtoPrimitive).create.commands.loneElement
    participant.ledger_api.javaapi.commands.submit(Seq(partyId), Seq(cycle), commandId = commandId)

  }
}
