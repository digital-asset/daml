// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.repair.RepairServiceError
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}

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
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
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
  }
}
