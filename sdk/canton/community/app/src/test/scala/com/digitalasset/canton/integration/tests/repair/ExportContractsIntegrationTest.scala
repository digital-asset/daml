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
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.data.RepairContract
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.google.protobuf.ByteString

final class ExportContractsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasProgrammableSequencer {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  def tryLoadFrom(acsSnapshot: File): List[RepairContract] =
    RepairContract
      .loadAcsSnapshot(ByteString.copyFrom(acsSnapshot.byteArray))
      .fold(errorMessage => throw new RuntimeException(errorMessage), identity)

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
          participant1.parties.export_acs(
            parties = Set(uninformed),
            exportFilePath = file.toString,
            filterSynchronizerId = Some(daId),
            ledgerOffset = uninformedOffset,
          )

          tryLoadFrom(file) should be(empty)
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
              participant1.parties
                .export_acs(
                  parties = Set(payer),
                  exportFilePath = file.toString,
                  filterSynchronizerId = Some(
                    SynchronizerId(UniqueIdentifier.tryCreate("synchronizer", "id"))
                  ), // `participant1` is only connected to `da`,
                  ledgerOffset = payerOffset,
                ),
              _.errorMessage should include(PartyManagementServiceError.InvalidArgument.id),
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
            participant1.parties
              .export_acs(
                parties = Set(bob),
                exportFilePath = dumpForAlice.canonicalPath,
                ledgerOffset = NonNegativeLong.tryCreate(ledgerEndP1),
              )
            participant2.parties
              .export_acs(
                parties = Set(alice),
                exportFilePath = dumpForBob.canonicalPath,
                ledgerOffset = NonNegativeLong.tryCreate(ledgerEndP2),
              )

            val forAlice = tryLoadFrom(dumpForAlice)
            val forBob = tryLoadFrom(dumpForBob)

            forAlice should have length 2

            forAlice should contain theSameElementsAs forBob
          }
      }
    }
  }

}
