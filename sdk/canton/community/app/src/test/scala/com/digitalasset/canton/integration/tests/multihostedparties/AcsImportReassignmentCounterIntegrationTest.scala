// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
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
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.transaction.ParticipantPermission

/** This test ensures that reassignment counter is not discarded upon ACS import.
  */
sealed trait AcsImportReassignmentCounterIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(CantonExamplesPath)

        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
        )
      }

  "Reassignment counter should be correct when importing the ACS" in { implicit env =>
    import env.*
    val alice = participant1.parties.enable("Alice", synchronizer = Some(daName))
    participant1.parties.enable("Alice", synchronizer = Some(acmeName))

    val iou = IouSyntax.createIou(participant1)(alice, alice)
    val cid = LfContractId.assertFromString(iou.id.contractId)
    participant1.ledger_api.commands.submit_reassign(alice, Seq(cid), daId, acmeId)

    val iouP1 = participant1.ledger_api.state.acs.active_contracts_of_party(alice).loneElement

    iouP1.reassignmentCounter shouldBe 1

    // Authorize Alice -> P2 on acme
    val ledgerEndP1 = participant1.ledger_api.state.end()
    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), acmeId)(
      owningParticipant = participant1,
      partyId = alice,
      threshold = PositiveInt.one,
      hosting = Set(
        (participant1, ParticipantPermission.Submission),
        (participant2, ParticipantPermission.Submission),
      ),
    )

    val exportOffset = participant1.parties.find_party_max_activation_offset(
      partyId = alice,
      participantId = participant2,
      synchronizerId = acmeId,
      beginOffsetExclusive = ledgerEndP1,
      completeAfter = PositiveInt.one,
    )

    val acsSnapshotPath = File.newTemporaryFile()

    participant1.parties.export_acs(
      Set(alice),
      exportOffset,
      Some(acmeId),
      acsSnapshotPath.canonicalPath,
    )
    participant2.synchronizers.disconnect_all()
    participant2.repair.import_acs(acsSnapshotPath.canonicalPath)
    participant2.synchronizers.reconnect_all()

    participant2.ledger_api.state.acs
      .active_contracts_of_party(alice)
      .loneElement
      .reassignmentCounter shouldBe 1
  }
}

final class AcsImportReassignmentCounterIntegrationTestPostgres
    extends AcsImportReassignmentCounterIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
}
