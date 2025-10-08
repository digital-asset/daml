// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedCreatedEvent
import com.digitalasset.canton.config
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.{Party, PartyId}

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
  ): Unit = {

    Seq(participant2, participant1).map { participant =>
      if (participant.packages.find_by_module("Cycle").isEmpty) {
        participant.dars.upload(CantonExamplesPath)
      }
    }

    participantAcs(participant2, partyId) shouldBe empty

    clue("creating cycle " + commandId) {
      createCycleContract(
        participant1,
        partyId,
        "I SHALL CREATE",
        commandId,
      )
    }

    val coid = participant2.ledger_api.javaapi.state.acs.await(M.Cycle.COMPANION)(partyId)
    clue("submitting response") {
      archiveCycleContract(
        participant2,
        partyId,
        coid,
        commandId,
      )
    }

    eventually() {
      participantAcs(participant2, partyId) shouldBe empty
    }
  }

  private def participantAcs(
      participant: ParticipantReference,
      partyId: PartyId,
  ): Seq[LedgerApiTypeWrappers.WrappedCreatedEvent] =
    participant.ledger_api.state.acs
      .of_party(partyId)
      .filter(_.templateId.isModuleEntity("Cycle", "Cycle"))
      .map(entry => WrappedCreatedEvent(entry.event))

  def createCycleCommand(party: Party, id: String): Command =
    Command.fromJavaProto(
      new Cycle(id, party.toProtoPrimitive)
        .create()
        .commands
        .loneElement
        .toProtoCommand
    )

  def cleanupCycles(
      partyId: PartyId,
      participant: ParticipantReference,
      commandId: String = "",
  ): Unit = {
    val coids = participant.ledger_api.javaapi.state.acs.filter(M.Cycle.COMPANION)(partyId)
    clue("submitting responses for cleanup") {
      archiveCycleContracts(participant, partyId, coids, commandId)
    }

    eventually() {
      participantAcs(participant, partyId) shouldBe empty
    }
  }

  def createCycleContract(
      participant: ParticipantReference,
      party: Party,
      id: String,
      commandId: String = "",
      optTimeout: Option[config.NonNegativeDuration] = Some(
        ConsoleCommandTimeout.defaultLedgerCommandsTimeout
      ),
  ): Cycle.Contract = {
    val cycle = new M.Cycle(id, party.toProtoPrimitive).create.commands.loneElement

    val tx = participant.ledger_api.javaapi.commands
      .submit(Seq(party), Seq(cycle), commandId = commandId, optTimeout = optTimeout)

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

  def archiveCycleContract(
      participant: ParticipantReference,
      partyId: PartyId,
      coid: Cycle.Contract,
      commandId: String = "",
  ): Unit = {
    val cycleEx = coid.id.exerciseArchive().commands.loneElement
    participant.ledger_api.javaapi.commands.submit(
      Seq(partyId),
      Seq(cycleEx),
      commandId = (if (commandId.isEmpty) "" else s"$commandId-response"),
    )
  }

  def archiveCycleContracts(
      participant: ParticipantReference,
      partyId: PartyId,
      coids: Seq[Cycle.Contract],
      commandId: String = "",
  ): Unit = {
    val cycleExs = coids.map(_.id.exerciseArchive().commands.loneElement)
    participant.ledger_api.javaapi.commands.submit(
      Seq(partyId),
      cycleExs,
      commandId = (if (commandId.isEmpty) "" else s"$commandId-responses"),
    )
  }
}
