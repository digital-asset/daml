// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
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
  *
  * Test: Replicate Alice to participant3
  *   - Authorize Alice on participant3
  *   - Export ACS for Alice on participant1
  *   - Import ACS for Alice on participant3
  *
  *   - Assert expected number of active contracts for Alice on participant3.
  *   - Assert operation can continue on participant3, that is Alice can archive and create
  *     contracts on participant3.
  */
final class OfflinePartyReplicationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  // A party gets activated on the multiple participants without being replicated (= ACS mismatch),
  // and we want to minimize the risk of warnings related to acs commitment mismatches
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
      import env.*
      participants.local.synchronizers.connect_local(sequencer1, daName)
      participants.local.dars.upload(CantonExamplesPath)
      sequencer1.topology.synchronizer_parameters
        .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))
    }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  private val acsFilename: String = "offline_party_replication_test_acs_snapshot.gz"

  override def afterAll(): Unit =
    try {
      val exportFile = File(acsFilename)
      if (exportFile.exists) {
        exportFile.delete()
      }
    } finally super.afterAll()

  "Exporting and importing a LAPI based ACS snapshot as part of a party replication" in {
    implicit env =>
      import env.*

      val alice = participant1.parties.enable(
        "Alice",
        synchronizeParticipants = Seq(participant2),
      )
      val bob = participant2.parties.enable(
        "Bob",
        synchronizeParticipants = Seq(participant1),
      )

      IouSyntax.createIou(participant1)(alice, bob, 1.95).discard
      IouSyntax.createIou(participant1)(alice, bob, 2.95).discard
      IouSyntax.createIou(participant1)(alice, bob, 3.95).discard
      IouSyntax.createIou(participant1)(alice, bob, 4.95).discard
      IouSyntax.createIou(participant1)(alice, bob, 5.95).discard

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
        exportFilePath = acsFilename,
      )

      participant3.synchronizers.disconnect_all()

      participant3.repair.import_acs(acsFilename, "", ContractIdImportMode.Accept)

      participant3.synchronizers.reconnect(daName)

      participant3.ledger_api.state.acs.active_contracts_of_party(alice) should have size 5

      // Archive contract and create another one to assert regular operation after the completed party replication
      val iou = participant3.ledger_api.javaapi.state.acs.filter(M.iou.Iou.COMPANION)(alice).head
      IouSyntax.archive(participant3)(iou, alice)

      IouSyntax.createIou(participant3)(alice, bob, 42.95).discard
  }

}
