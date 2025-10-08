// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId

// TODO(#23073) - Remove this test once #27325 has been re-implemented
final class ImportContractsIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

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
        alice = participant1.parties.enable("Alice", synchronizer = daName)
        participant1.parties.enable("Alice", synchronizer = acmeName)

        bob = participant2.parties.enable("Bob", synchronizer = daName)
        participant2.parties.enable("Bob", synchronizer = acmeName)
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

      val contract = IouSyntax.createIou(participant1)(alice, bob)
      val reassignedContractCid = LfContractId.assertFromString(contract.id.contractId)

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

      participant1.health.ping(participant1)

      File.usingTemporaryFile() { file =>
        participant1.repair.export_acs(
          parties = Set(alice),
          exportFilePath = file.toString,
          synchronizerId = Some(daId),
          ledgerOffset = NonNegativeLong.tryCreate(participant1.ledger_api.state.end()),
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
