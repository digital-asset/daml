// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.participant.admin.repair.RepairServiceError
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}

import scala.annotation.nowarn

@nowarn("cat=deprecation") // Usage of old acs export
final class ExportContractsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*

        // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
        // Adding the "owner" party to P3 can expectedly trigger the ACS_MISMATCH_NO_SHARED_CONTRACTS
        // commitment warning because the test exports, but does not import contracts.
        // Set `reconciliationInterval` to ten years to avoid associated test flakes.
        sequencer1.topology.synchronizer_parameters
          .propose_update(
            daId,
            _.update(reconciliationInterval = PositiveSeconds.tryOfDays(10 * 365).toConfig),
          )

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  "Exporting an ACS" should {

    "return an empty ACS if the party is privy to no contract" in { implicit env =>
      import env.*

      WithEnabledParties(
        participant1 -> Seq("uninformed", "payer"),
        participant2 -> Seq("owner"),
      ) { case Seq(uninformed, payer, owner) =>
        IouSyntax.createIou(participant1)(payer, owner)

        val uninformedOffset = participant1.parties.find_party_max_activation_offset(
          partyId = uninformed,
          participantId = participant1.id,
          synchronizerId = daId,
          completeAfter = PositiveInt.one,
        )

        File.usingTemporaryFile() { file =>
          participant1.repair.export_acs(
            parties = Set(uninformed),
            exportFilePath = file.toString,
            synchronizerId = Some(daId),
            ledgerOffset = uninformedOffset,
          )

          repair.acs.read_from_file(file.canonicalPath) shouldBe empty
        }
      }
    }

    "fail export when using an unknown synchronizer filter" in { implicit env =>
      import env.*
      WithEnabledParties(participant1 -> Seq("payer"), participant2 -> Seq("owner")) {
        case Seq(payer, owner) =>
          IouSyntax.createIou(participant1)(payer, owner)

          val payerOffset = participant1.parties.find_party_max_activation_offset(
            partyId = payer,
            participantId = participant1.id,
            synchronizerId = daId,
            completeAfter = PositiveInt.one,
          )

          File.usingTemporaryFile() { file =>
            loggerFactory.assertThrowsAndLogs[CommandFailure](
              participant1.repair
                .export_acs(
                  parties = Set(payer),
                  exportFilePath = file.toString,
                  synchronizerId = Some(
                    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer", "id"))
                  ), // `participant1` is only connected to `da`,
                  ledgerOffset = payerOffset,
                ),
              _.errorMessage should include(RepairServiceError.InvalidArgument.id),
            )
            file.exists shouldBe false // file should be deleted if error
          }
      }
    }

    "export the contracts with the correct visibility across participants" in { implicit env =>
      import env.*

      WithEnabledParties(participant1 -> Seq("alice"), participant2 -> Seq("bob")) {
        case enabledParties @ Seq(alice, bob) =>
          for {
            (participant, payer) <- Seq((participant1, alice), (participant2, bob))
            owner <- enabledParties
          } {
            IouSyntax.createIou(participant)(payer, owner)
          }

          for {
            dumpForAlice <- File.temporaryFile()
            dumpForBob <- File.temporaryFile()
          } {

            // contracts are in the ACS at the these offsets
            val ledgerEndP1 = participant1.ledger_api.state.end()
            val ledgerEndP2 = participant2.ledger_api.state.end()

            // exporting contracts from the participant where they are not hosted
            participant1.repair
              .export_acs(
                parties = Set(bob),
                exportFilePath = dumpForAlice.canonicalPath,
                ledgerOffset = NonNegativeLong.tryCreate(ledgerEndP1),
              )
            participant2.repair
              .export_acs(
                parties = Set(alice),
                exportFilePath = dumpForBob.canonicalPath,
                ledgerOffset = NonNegativeLong.tryCreate(ledgerEndP2),
              )

            val forAlice = repair.acs
              .read_from_file(dumpForAlice.canonicalPath)
              .map(_.getCreatedEvent.contractId)
            val forBob =
              repair.acs.read_from_file(dumpForBob.canonicalPath).map(_.getCreatedEvent.contractId)

            forAlice should have length 2
            forAlice should contain theSameElementsAs forBob
          }
      }
    }

    "allow exporting all contracts with a wildcard filter" in { implicit env =>
      import env.*

      for {
        explicitExport <- File.temporaryFile()
        wildcardExport <- File.temporaryFile()
        explicitExportOld <- File.temporaryFile()
        wildcardExportOld <- File.temporaryFile()
      } {
        val ledgerEndP1 = participant1.ledger_api.state.end()
        val p1Parties = participant1.parties
          .list(filterParticipant = participant1.filterString)
          .map(_.party)
          .toSet

        // export with parties explicitly specified
        participant1.repair.export_acs(
          parties = p1Parties,
          exportFilePath = explicitExport.canonicalPath,
          ledgerOffset = NonNegativeLong.tryCreate(ledgerEndP1),
        )
        participant1.repair.export_acs_old(
          parties = p1Parties,
          partiesOffboarding = false,
          outputFile = explicitExportOld.canonicalPath,
        )

        // export contracts for all parties with the wildcard filter
        participant1.repair.export_acs(
          parties = Set.empty,
          exportFilePath = wildcardExport.canonicalPath,
          ledgerOffset = NonNegativeLong.tryCreate(ledgerEndP1),
        )
        participant1.repair.export_acs_old(
          parties = Set.empty,
          partiesOffboarding = false,
          outputFile = wildcardExportOld.canonicalPath,
        )

        val forExplicit = repair.acs
          .read_from_file(explicitExport.canonicalPath)
          .map(_.getCreatedEvent.contractId)
        val forExplicitOld = ActiveContractOld
          .loadFromByteString(utils.read_byte_string_from_file(explicitExportOld.canonicalPath))
          .value
          .map(_.contract.contractId.coid)

        val forWildcard = repair.acs
          .read_from_file(wildcardExport.canonicalPath)
          .map(_.getCreatedEvent.contractId)
        val forWildcardOld = ActiveContractOld
          .loadFromByteString(utils.read_byte_string_from_file(wildcardExportOld.canonicalPath))
          .value
          .map(_.contract.contractId.coid)

        forExplicit should not be empty
        forExplicit should contain theSameElementsAs forWildcard
        forExplicit should contain theSameElementsAs forExplicitOld
        forExplicit should contain theSameElementsAs forWildcardOld
      }
    }
  }
}
