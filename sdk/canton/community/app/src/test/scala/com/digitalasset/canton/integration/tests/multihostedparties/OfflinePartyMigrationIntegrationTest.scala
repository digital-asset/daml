// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.AcsInspection
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.{ReassignmentCounter, config}

import scala.jdk.CollectionConverters.*

trait OfflinePartyMigrationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection {

  private val aliceName = "Alice"
  private val bobName = "Bob"
  private val charlieName = "Charlie"

  private var alice: PartyId = _
  private var bob: PartyId = _
  private var charlie: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M1_S2M1
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer2, alias = daName)
        participant3.synchronizers.connect_local(sequencer2, alias = daName)

        participant1.synchronizers.connect_local(sequencer3, alias = acmeName)
        participant2.synchronizers.connect_local(sequencer4, alias = acmeName)
        participant3.synchronizers.connect_local(sequencer4, alias = acmeName)

        participants.all.dars.upload(CantonExamplesPath)

        // Allocate parties
        alice = participant1.parties.enable(
          aliceName,
          synchronizeParticipants = Seq(participant2, participant3),
          synchronizer = daName,
        )
        participant1.parties.enable(
          aliceName,
          synchronizeParticipants = Seq(participant2, participant3),
          synchronizer = acmeName,
        )

        bob = participant2.parties.enable(
          bobName,
          synchronizeParticipants = Seq(participant1, participant3),
          synchronizer = daName,
        )
        participant2.parties.enable(
          bobName,
          synchronizeParticipants = Seq(participant1, participant3),
          synchronizer = acmeName,
        )

        charlie = participant3.parties.enable(
          charlieName,
          synchronizeParticipants = Seq(participant1, participant2),
          synchronizer = daName,
        )
        participant3.parties.enable(
          charlieName,
          synchronizeParticipants = Seq(participant1, participant2),
          synchronizer = acmeName,
        )

      }

  private val acsFilename: String = "alize.gz"

  override def afterAll(): Unit =
    try {
      File(acsFilename).delete()
    } finally super.afterAll()

  "setup our test scenario: create archived and active contracts" in { implicit env =>
    import env.*

    Seq(
      (alice, bob, participant1),
      (alice, charlie, participant1),
      (bob, charlie, participant2),
      (bob, alice, participant2),
    )
      .foreach { case (obligor, owner, participant) =>
        // create one contract via create & exercise
        val iou = IouSyntax.createIou(participant, Some(daId))(obligor, obligor)
        participant.ledger_api.javaapi.commands
          .submit(
            Seq(obligor),
            iou.id.exerciseTransfer(owner.toProtoPrimitive).commands.asScala.toSeq,
            Some(daId),
          )
        // also, create one directly
        IouSyntax.createIou(participant, Some(daId))(obligor, owner)
      }
  }

  private var reassignedContractCid: LfContractId = _
  "setup our test scenario: reassign Alice's active contract to another synchronizer, and back again to increment its reassignment counter" in {
    implicit env =>
      import env.*

      reassignedContractCid = findIOU(participant1, alice, bob).id.toLf

      participant1.synchronizers.connect_local(sequencer4, alias = acmeName)
      participant2.synchronizers.connect_local(sequencer4, alias = acmeName)

      participant1.testing.fetch_synchronizer_times()
      participant2.testing.fetch_synchronizer_times()

      participant1.ledger_api.commands.submit_reassign(
        alice,
        Seq(reassignedContractCid),
        daId,
        acmeId,
        submissionId = "some-submission-id",
      )

      participant1.ledger_api.commands.submit_reassign(
        alice,
        Seq(reassignedContractCid),
        acmeId,
        daId,
        submissionId = "some-submission-id",
      )
  }

  "move alice from p1 to p3, step 1" in { implicit env =>
    import env.*

    // disable ACS commitments by having a large reconciliation interval
    // do this on all synchronizers with participants connected that perform party migrations
    // TODO(#8583) remove when repair service can be fed with the timestamp of the ACS upload
    val daSynchronizerOwnersDa = Seq[InstanceReference](sequencer1, sequencer2)
    val daSynchronizerOwnersAcme = Seq[InstanceReference](sequencer3, sequencer4)

    daSynchronizerOwnersDa.foreach(
      _.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
      )
    )

    daSynchronizerOwnersAcme.foreach(
      _.topology.synchronizer_parameters.propose_update(
        synchronizerId = acmeId,
        _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
      )
    )

    repair.party_migration.step1_hold_and_store_acs(
      alice,
      partiesOffboarding = true,
      participant1,
      participant3.id,
      acsFilename,
    )

    // delegation should exist
    participant1.topology.party_to_participant_mappings
      .list(synchronizerId = daId, filterParty = alice.filterString, proposals = true)
      .flatMap(result =>
        result.item.participants.map(p => (result.item.partyId, p.participantId))
      ) should contain((alice, participant3.id))

    // but party should not be hosted anymore
    participant1.parties.hosted(aliceName) shouldBe empty

    participant1.health.ping(participant3.id)

  }

  "move alice from p1 to p3, step 2" in { implicit env =>
    import env.*
    repair.party_migration.step2_import_acs(alice, participant3, acsFilename)

    participant1.health.ping(participant3.id)
  }

  "move alice from p1 to p3, step 3" in { implicit env =>
    import env.*

    participant3.parties
      .list(aliceName)
      .flatMap(_.participants.map(_.participant))
      .distinct
      .toList shouldBe Seq(participant3.id)

    val contracts = participant3.ledger_api.state.acs.of_party(alice)

    contracts should have length 6

    val acs = participant3.underlying.value.sync.stateInspection
      .findAcs(daName)
      .valueOrFail(s"get ACS on $daName for $participant3")
      .futureValueUS

    val (_, counter) =
      acs.get(reassignedContractCid).valueOrFail("get contract with 2 reassignments")

    withClue("Reassignment counter should be two after two reassignments") {
      counter shouldBe ReassignmentCounter(2)
    }

    val transfer = findIOU(participant3, alice, _.data.owner == alice.toProtoPrimitive)

    val boris = participant1.parties.enable(
      "Boris",
      synchronizeParticipants = Seq(participant3),
      synchronizer = daName,
    )
    participant1.parties.enable(
      "Boris",
      synchronizeParticipants = Seq(participant3),
      synchronizer = acmeName,
    )

    participant3.ledger_api.javaapi.commands
      .submit(
        Seq(alice),
        transfer.id.exerciseTransfer(boris.toProtoPrimitive).commands.asScala.toSeq,
        Some(daId),
      )

    participant1.ledger_api.state.acs.of_party(boris) should have length 1
  }

  "move alice from p1 to p3, step 4" in { implicit env =>
    import env.*

    val alice = participant1.parties.find(aliceName)
    participant1.synchronizers.disconnect_all()
    repair.party_migration.step4_clean_up_source(alice, participant1, acsFilename)
    participant1.synchronizers.reconnect_all()

    eventually() {
      participant1.ledger_api.state.acs.of_party(alice) shouldBe empty
    }
  }

}

class OfflinePartyMigrationIntegrationTestPostgres extends OfflinePartyMigrationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1"), InstanceName.tryCreate("sequencer2")),
          Set(InstanceName.tryCreate("sequencer3"), InstanceName.tryCreate("sequencer4")),
        )
      ),
    )
  )
}
