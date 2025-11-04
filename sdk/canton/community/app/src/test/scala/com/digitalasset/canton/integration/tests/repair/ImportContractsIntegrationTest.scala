// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission

// TODO(#23073) - Remove this test once #27325 has been re-implemented
final class ImportContractsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  private var alice: PartyId = _
  private var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        PartiesAllocator(Set(participant1, participant2))(
          newParties = Seq("Alice" -> participant1, "Bob" -> participant2),
          targetTopology = Map(
            "Alice" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission)),
              acmeId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission)),
            ),
            "Bob" -> Map(
              daId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission)),
              acmeId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission)),
            ),
          ),
        )

        alice = "Alice".toPartyId(participant1)
        bob = "Bob".toPartyId(participant2)
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )

  "Importing an ACS" should {

    "fail for contracts with non-zero reassignment counter" in { implicit env =>
      import env.*

      val contract = IouSyntax.createIou(participant1, synchronizerId = Some(daId))(alice, bob)
      val reassignedContractCid = LfContractId.assertFromString(contract.id.contractId)

      participant1.ledger_api.commands.submit_reassign(
        alice,
        Seq(reassignedContractCid),
        source = daId,
        target = acmeId,
        submissionId = "some-submission-id",
      )

      participant1.ledger_api.commands.submit_reassign(
        alice,
        Seq(reassignedContractCid),
        source = acmeId,
        target = daId,
        submissionId = "some-submission-id",
      )

      participant1.health.ping(participant1)

      File.usingTemporaryFile() { file =>
        participant1.repair.export_acs(
          parties = Set(alice),
          exportFilePath = file.toString,
          synchronizerId = Some(daId),
          ledgerOffset = participant1.ledger_api.state.end(),
        )

        val contracts = repair.acs.read_from_file(file.canonicalPath)
        contracts.loneElement.reassignmentCounter shouldBe 2

        assertThrowsAndLogsCommandFailures(
          participant3.repair.import_acs(file.canonicalPath),
          _.errorMessage should include(
            "at least one contract with a non-zero reassignment counter"
          ),
        )
      }
    }
  }
}
