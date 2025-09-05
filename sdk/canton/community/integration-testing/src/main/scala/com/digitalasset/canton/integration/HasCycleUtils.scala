// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedCreatedEvent
import com.digitalasset.canton.config
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.{ExternalParty, Party, PartyId}

/** Adds the ability to run cycles to integration tests
  */
trait HasCycleUtils {
  this: BaseIntegrationTest =>

  /** @param partyId
    *   assumes that the party is hosted on participant1 AND participant2 (in the simplest case this
    *   could simply mean that participant1 == participant2)
    */
  def runCycle(
      partyId: PartyId,
      participant1: ParticipantReference,
      participant2: ParticipantReference,
      commandId: String = "",
  )(implicit env: TestConsoleEnvironment): Unit = {

    Seq(participant2, participant1).map { participant =>
      if (participant.packages.find_by_module("Cycle").isEmpty) {
        participant.dars.upload(CantonExamplesPath)
      }
    }

    def p2acs(): Seq[LedgerApiTypeWrappers.WrappedCreatedEvent] =
      participant2.ledger_api.state.acs
        .of_party(partyId)
        .filter(_.templateId.isModuleEntity("Cycle", "Cycle"))
        .map(entry => WrappedCreatedEvent(entry.event))

    p2acs() shouldBe empty

    clue("creating cycle " + commandId) {
      createCycleContract(participant1, partyId, "I SHALL CREATE", commandId)
    }
    val coid = participant2.ledger_api.javaapi.state.acs.await(M.Cycle.COMPANION)(partyId)
    val cycleEx = coid.id.exerciseArchive().commands.loneElement
    clue("submitting response") {
      participant2.ledger_api.javaapi.commands.submit(
        Seq(partyId),
        Seq(cycleEx),
        commandId = (if (commandId.isEmpty) "" else s"$commandId-response"),
      )
    }
    eventually() {
      p2acs() shouldBe empty
    }
  }

  // TODO(#27482): This can be simplified once helpers are in main
  def createCycleContract(
      participant: ParticipantReference,
      party: Party,
      id: String,
      commandId: String = "",
      optTimeout: Option[config.NonNegativeDuration] = Some(
        ConsoleCommandTimeout.defaultLedgerCommandsTimeout
      ),
  )(implicit env: TestConsoleEnvironment): Cycle.Contract = {
    import env.*

    val cycle = new M.Cycle(id, party.toProtoPrimitive).create.commands.loneElement

    val tx = party match {
      case partyE: ExternalParty =>
        participant.external_parties.ledger_api.javaapi.commands.submit(
          partyE,
          Seq(new Cycle(id, party.toProtoPrimitive).create().commands().loneElement),
          commandId = commandId,
          optTimeout = optTimeout,
        )

      case _: PartyId =>
        participant.ledger_api.javaapi.commands
          .submit(Seq(party.partyId), Seq(cycle), commandId = commandId, optTimeout = optTimeout)
    }

    JavaDecodeUtil.decodeAllCreated(Cycle.COMPANION)(tx).loneElement
  }

  def createCycleContracts(
      participant: ParticipantReference,
      partyId: PartyId,
      ids: Seq[String],
      commandId: String = "",
      optTimeout: Option[config.NonNegativeDuration] = Some(
        ConsoleCommandTimeout.defaultLedgerCommandsTimeout
      ),
  ): Unit = {
    if (participant.packages.find_by_module("Cycle").isEmpty) {
      participant.dars.upload(CantonExamplesPath)
    }
    val cycles = ids.map(new M.Cycle(_, partyId.toProtoPrimitive).create.commands.loneElement)
    participant.ledger_api.javaapi.commands
      .submit(Seq(partyId), cycles, commandId = commandId, optTimeout = optTimeout)
  }
}
