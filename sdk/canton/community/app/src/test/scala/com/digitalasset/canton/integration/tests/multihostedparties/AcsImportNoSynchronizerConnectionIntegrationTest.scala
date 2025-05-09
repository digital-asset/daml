// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission

import scala.jdk.CollectionConverters.*

/*
In this test, we check that we can perform an ACS import on a fresh participant with a
pure register. The scenario is the following:
- P1 hosts Alice, two contracts are created
- P1 exports the ACS
- P2 registers the synchronizer without connecting to it
  - handshake is done, static synchronizer parameters are retrieved
  - the synchronizer is not started
- P2 imports the ACS
- P2 connects to the synchronizer
- Alice is authorized on P2
- One contract is archived

About P3:
We have P3 playing a similar role than P2 but with manualConnect=true (because of issues we had on CN).

Reason for this test:
In the major upgrade for early MainNet, we want to be sure that the participant does not get
transactions to process before it successfully imported the ACS snapshot.
 */
sealed trait AcsImportNoSynchronizerConnectionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))

  private val aliceName = "Alice"
  private var alice: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
      import env.*
      acsFilename.deleteOnExit()

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.dars.upload(CantonExamplesPath)

      // Allocate parties
      alice = participant1.parties.enable(aliceName)
    }

  private lazy val acsFilename: File =
    File.temporaryFile("AcsImportNoSynchronizerConnectionIntegrationTest").get()
  private val coinsAmount: Seq[Double] = Seq(1.0, 2.0)

  "create contracts and export ACS" in { implicit env =>
    import env.*

    coinsAmount.foreach { amount =>
      IouSyntax.createIou(participant1)(alice, alice, amount = amount)
    }

    val aliceAcsOffset = NonNegativeLong.tryCreate(
      participant1.ledger_api.state.acs
        .active_contracts_of_party(alice)
        .lastOption
        .value
        .createdEvent
        .value
        .offset
    )

    participant1.parties
      .export_acs(
        parties = Set(alice),
        exportFilePath = acsFilename.canonicalPath,
        ledgerOffset = aliceAcsOffset,
      )
  }

  "register synchronizer" should {
    "take handshake only flag into account" in { implicit env =>
      import env.*

      participant2.synchronizers.register(sequencer1, daName, manualConnect = false)
      participant2.synchronizers.list_registered().map { case (config, _, _) =>
        config.synchronizerAlias
      } shouldBe Seq(daName)
      participant2.synchronizers.list_connected() shouldBe empty

      participant3.synchronizers.register(sequencer1, daName, manualConnect = true)
      participant3.synchronizers.list_registered().map { case (config, _, _) =>
        config.synchronizerAlias
      } shouldBe Seq(daName)

      participant3.synchronizers.list_connected() shouldBe empty
    }

    "allow importing the ACS" in { implicit env =>
      import env.*

      participant2.dars.upload(CantonExamplesPath)
      participant3.dars.upload(CantonExamplesPath)

      participant2.repair.import_acs(acsFilename.canonicalPath)
      participant3.repair.import_acs(acsFilename.canonicalPath)

      participants.all.synchronizers.reconnect_all()

      Seq(participant1, participant2).foreach(
        _.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          adds = List(participant2.id -> ParticipantPermission.Submission),
          store = daId,
        )
      )

      eventually() {
        participant1.topology.party_to_participant_mappings.is_known(
          daId,
          alice,
          Seq(participant2),
        ) shouldBe true

        participant2.topology.party_to_participant_mappings.is_known(
          daId,
          alice,
          Seq(participant2),
        ) shouldBe true
      }
    }
  }

  "contracts can be archived" in { implicit env =>
    import env.*

    val initialAcsSize = coinsAmount.size.toLong
    val finalAcsSize = coinsAmount.size.toLong - 1

    participant1.ledger_api.state.acs.of_party(alice) should have size initialAcsSize
    participant2.ledger_api.state.acs.of_party(alice) should have size initialAcsSize

    val iou = participant2.ledger_api.javaapi.state.acs.filter(Iou.COMPANION)(alice).head

    participant2.ledger_api.javaapi.commands
      .submit(Seq(alice), iou.id.exerciseArchive().commands().asScala.toSeq)

    participant1.ledger_api.state.acs.of_party(alice) should have size finalAcsSize
    participant2.ledger_api.state.acs.of_party(alice) should have size finalAcsSize
  }
}

class AcsImportNoSynchronizerConnectionIntegrationTestPostgres
    extends AcsImportNoSynchronizerConnectionIntegrationTest
