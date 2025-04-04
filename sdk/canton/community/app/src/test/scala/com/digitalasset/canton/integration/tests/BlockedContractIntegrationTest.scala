// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.UnknownInformees
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
import com.digitalasset.canton.topology.{ForceFlags, PartyId}

import scala.jdk.CollectionConverters.*

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
sealed trait BlockedContractIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var bank: PartyId = _
  private var alice: PartyId = _
  private var bob: PartyId = _

  private val amount: Amount = new Amount(100.toBigDecimal, "USD")

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonExamplesPath)

      bank = participant1.parties.enable("Bank")
      alice = participant1.parties.enable("Alice")
      bob = participant2.parties.enable("Bob")
    }

  "A contract can be archived if a participant just disconnects" in { implicit env =>
    import env.*

    // Create Iou
    val iou = submitAndReturnIou(participant1, bank, createIouWithObserver(bob))

    // Check that the observer sees the Iou
    eventually() {
      participant2.ledger_api.state.acs.of_party(bob) should have size 1
    }

    participant2.synchronizers.disconnect(daName)

    submitAndReturnIou(
      participant1,
      alice,
      iou.id.exerciseTransfer(bank.toProtoPrimitive).commands.loneElement,
      autoSync = false,
    )

    // The observer still sees the Iou, as it has been disconnected
    eventuallyForever() {
      participant2.ledger_api.state.acs.of_party(bob) should have size 1
    }

    participant2.synchronizers.reconnect_all()

    // The Iou gets archived after reconnecting.
    eventually() {
      participant2.ledger_api.state.acs.of_party(bob) shouldBe empty
    }
  }

  "A contract can't be archived after an observer has been disabled" in { implicit env =>
    import env.*

    val iou = submitAndReturnIou(participant1, bank, createIouWithObserver(bob))

    participant2.topology.party_to_participant_mappings
      .propose_delta(
        bob,
        removes = List(participant2),
        forceFlags = ForceFlags(DisablePartyWithActiveContracts),
      )

    // Wait until participant1 observes Bob having been disabled
    eventually() {
      participant1.parties.list(filterParty = "Bob") shouldBe Seq.empty
    }

    // FIXME(i3418): A participant can block a contract by disabling an observer.
    assertThrowsAndLogsCommandFailures(
      submitAndReturnIou(
        participant1,
        alice,
        iou.id.exerciseTransfer(bank.toProtoPrimitive).commands.loneElement,
      ),
      _.commandFailureMessage should include(UnknownInformees.id),
    )
  }

  def createIouWithObserver(observer: PartyId): Command =
    new Iou(
      bank.toProtoPrimitive,
      alice.toProtoPrimitive,
      amount,
      List(alice.toProtoPrimitive, observer.toProtoPrimitive).asJava,
    ).create.commands.loneElement

  def submitAndReturnIou(
      participantRef: ParticipantReference,
      submitter: PartyId,
      command: Command,
      autoSync: Boolean = true,
  ): Iou.Contract =
    if (autoSync)
      JavaDecodeUtil
        .decodeAllCreatedTree(Iou.COMPANION)(
          participantRef.ledger_api.javaapi.commands.submit(Seq(submitter), Seq(command))
        )
        .loneElement
    else
      JavaDecodeUtil
        .decodeAllCreatedTree(Iou.COMPANION)(
          participantRef.ledger_api.javaapi.commands
            .submit(Seq(submitter), Seq(command), optTimeout = None)
        )
        .loneElement
}

//class BlockedContractTestDefault extends BlockedContractTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}

class BlockedContractIntegrationTestPostgres extends BlockedContractIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
