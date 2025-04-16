// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.daml.ledger.api.v2.state_service.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
import com.daml.ledger.api.v2.topology_transaction.{ParticipantAuthorizationAdded, TopologyEvent}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.data.ContractIdImportMode
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP

/** Setup:
  *   - Alice is hosted on participant1
  *   - Bob is hosted on participant2
  *   - 5 active IOU contracts between Alice and Bob
  *
  * Test ACS export/import specifying the ledger offset for the export: Replicate Alice to
  * participant3
  *   - Authorize Alice on participant3
  *   - Export ACS for Alice on participant1
  *   - Import ACS for Alice on participant3
  *
  *   - Assert expected number of active contracts for Alice on participant3.
  *   - Assert operation can continue on participant3, that is Alice can archive and create
  *     contracts on participant3.
  *
  * Test ACS export/import specifying the topology transaction effective time for the export:
  * Replicate Alice to participant4
  *   - Authorize Alice on participant4
  *   - Export ACS for Alice on participant1
  *   - Import ACS for Alice on participant4
  *
  *   - Assert expected number of active contracts for Alice on participant4.
  *   - Assert operation can continue on participant4, that is Alice can archive and create
  *     contracts on participant4.
  */
final class OfflinePartyReplicationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  // A party gets activated on the multiple participants without being replicated (= ACS mismatch),
  // and we want to minimize the risk of warnings related to acs commitment mismatches
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1.withSetup { implicit env =>
      import env.*
      participants.local.synchronizers.connect_local(sequencer1, daName)
      participants.local.dars.upload(CantonExamplesPath)
      sequencer1.topology.synchronizer_parameters
        .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))

      val alice = participant1.parties.enable("Alice", synchronizeParticipants = Seq(participant2))
      val bob = participant2.parties.enable("Bob", synchronizeParticipants = Seq(participant1))

      IouSyntax.createIou(participant1)(alice, bob, 1.95).discard
      IouSyntax.createIou(participant1)(alice, bob, 2.95).discard
      IouSyntax.createIou(participant1)(alice, bob, 3.95).discard
      IouSyntax.createIou(participant1)(alice, bob, 4.95).discard
      IouSyntax.createIou(participant1)(alice, bob, 5.95).discard
    }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  private val acsSnapshotAtOffset: String =
    "offline_party_replication_test_acs_snapshot_at_offset.gz"
  private val acsSnapshotAtTimestamp: String =
    "offline_party_replication_test_acs_snapshot_at_timestamp.gz"

  override def afterAll(): Unit =
    try {
      val exportFiles = Seq(File(acsSnapshotAtOffset), File(acsSnapshotAtTimestamp))
      exportFiles.foreach(file => if (file.exists) file.delete())
    } finally super.afterAll()

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication using ledger offset" in {
    implicit env =>
      import env.*

      val alice = participant1.parties.find("Alice")
      val bob = participant2.parties.find("Bob")

      val ledgerEndP1 = participant1.ledger_api.state.end()

      PartyToParticipantDeclarative.forParty(Set(participant1, participant3), daId)(
        participant1,
        alice,
        PositiveInt.one,
        Set(
          (participant1, PP.Submission),
          (participant3, PP.Submission),
        ),
      )

      val aliceAddedOnP3Offset = participant1.parties.find_party_max_activation_offset(
        partyId = alice,
        participantId = participant3.id,
        synchronizerId = daId,
        beginOffsetExclusive = ledgerEndP1,
        completeAfter = PositiveInt.one,
      )

      participant1.parties.export_acs(
        Set(alice),
        ledgerOffset = aliceAddedOnP3Offset,
        exportFilePath = acsSnapshotAtOffset,
      )

      participant3.synchronizers.disconnect_all()

      participant3.repair.import_acs(acsSnapshotAtOffset, "", ContractIdImportMode.Accept)

      participant3.synchronizers.reconnect(daName)

      participant3.ledger_api.state.acs.active_contracts_of_party(alice) should have size 5

      // Archive contract and create another one to assert regular operation after the completed party replication
      val iou = participant3.ledger_api.javaapi.state.acs.filter(M.iou.Iou.COMPANION)(alice).head
      IouSyntax.archive(participant3)(iou, alice)

      IouSyntax.createIou(participant3)(alice, bob, 42.95).discard
  }

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication using topology transaction effective time" in {
    implicit env =>
      import env.*

      val alice = participant1.parties.find("Alice")
      val bob = participant2.parties.find("Bob")

      val ledgerEndP1 = participant1.ledger_api.state.end()

      PartyToParticipantDeclarative.forParty(Set(participant1, participant4), daId)(
        participant1,
        alice,
        PositiveInt.one,
        Set(
          (participant1, PP.Submission),
          (participant4, PP.Submission),
        ),
      )

      participant1.ledger_api.updates
        .topology_transactions(
          completeAfter = PositiveInt.one,
          partyIds = Seq(alice),
          beginOffsetExclusive = ledgerEndP1,
          synchronizerFilter = Some(daId),
        )
        .loneElement
        .topologyTransaction
        .events
        .maxByOption(_.event.number)
        .value
        .event shouldBe TopologyEvent.Event.ParticipantAuthorizationAdded(
        ParticipantAuthorizationAdded(
          partyId = alice.toProtoPrimitive,
          participantId = participant4.uid.toProtoPrimitive,
          participantPermission = PARTICIPANT_PERMISSION_SUBMISSION,
        )
      )

      val onboardingTx = participant1.topology.party_to_participant_mappings
        .list(
          synchronizerId = daId,
          filterParty = alice.filterString,
          filterParticipant = participant4.filterString,
        )
        .loneElement
        .context

      participant1.parties.export_acs_at_timestamp(
        Set(alice),
        daId,
        onboardingTx.validFrom,
        exportFilePath = acsSnapshotAtTimestamp,
      )

      participant4.synchronizers.disconnect_all()

      participant4.repair.import_acs(acsSnapshotAtTimestamp, "", ContractIdImportMode.Accept)

      participant4.synchronizers.reconnect(daName)

      participant4.ledger_api.state.acs.active_contracts_of_party(alice) should have size 5

      // Archive contract and create another one to assert regular operation after the completed party replication
      val iou = participant4.ledger_api.javaapi.state.acs.filter(M.iou.Iou.COMPANION)(alice).head
      IouSyntax.archive(participant4)(iou, alice)

      IouSyntax.createIou(participant4)(alice, bob, 42.95).discard
  }

}
