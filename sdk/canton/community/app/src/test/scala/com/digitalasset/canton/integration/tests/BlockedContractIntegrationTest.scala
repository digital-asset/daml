// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.UnknownInformees
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
import com.digitalasset.canton.topology.{ForceFlags, Party}

import scala.jdk.CollectionConverters.*

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
sealed trait BlockedContractIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var bank: Party = _
  private var alice: Party = _
  private var bob: Party = _

  private val amount: Amount = new Amount(100.toBigDecimal, "USD")

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonExamplesPath)

      bank = participant1.parties.testing.enable("Bank")
      alice = participant1.parties.testing.enable("Alice")
      bob = participant2.parties.testing.enable("Bob")
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

    bob.topology.party_to_participant_mappings
      .propose_delta(
        participant2,
        removes = List(participant2),
        forceFlags = ForceFlags(DisablePartyWithActiveContracts),
        store = daId,
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

  def createIouWithObserver(observer: Party): Command =
    new Iou(
      bank.toProtoPrimitive,
      alice.toProtoPrimitive,
      amount,
      List(alice.toProtoPrimitive, observer.toProtoPrimitive).asJava,
    ).create.commands.loneElement

  def submitAndReturnIou(
      participantRef: ParticipantReference,
      submitter: Party,
      command: Command,
      autoSync: Boolean = true,
  ): Iou.Contract =
    if (autoSync)
      JavaDecodeUtil
        .decodeAllCreated(Iou.COMPANION)(
          participantRef.ledger_api.javaapi.commands.submit(Seq(submitter), Seq(command))
        )
        .loneElement
    else
      JavaDecodeUtil
        .decodeAllCreated(Iou.COMPANION)(
          participantRef.ledger_api.javaapi.commands
            .submit(
              Seq(submitter),
              Seq(command),
              optTimeout = None,
              transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            )
        )
        .loneElement
}

//class BlockedContractTestDefault extends BlockedContractTest {
// 	registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(new UseBftSequencer(loggerFactory))
//}

class BlockedContractIntegrationTestPostgres extends BlockedContractIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
