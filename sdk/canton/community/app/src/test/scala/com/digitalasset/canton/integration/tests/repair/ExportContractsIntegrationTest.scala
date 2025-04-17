// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import com.daml.ledger.api.v2.state_service.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
import com.daml.ledger.api.v2.topology_transaction.{ParticipantAuthorizationAdded, TopologyEvent}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{EntitySyntax, PartyToParticipantDeclarative}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.data.RepairContract
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.google.protobuf.ByteString

import java.time.Instant

final class ExportContractsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

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
            synchronizerId = Some(daId),
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
                  synchronizerId = Some(
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

    "export ACS at the effective time of a party onboarding topology transaction" in {
      implicit env =>
        import env.*
        WithEnabledParties(participant1 -> Seq("payer"), participant2 -> Seq("owner")) {
          case Seq(payer, owner) =>
            IouSyntax.createIou(participant1)(payer, owner)

            val ledgerOffsetBeforePartyOnboarding = participant2.ledger_api.state.end()

            PartyToParticipantDeclarative.forParty(Set(participant2, participant3), daId)(
              participant2,
              owner,
              PositiveInt.one,
              Set(
                (participant2, PP.Submission),
                (participant3, PP.Submission),
              ),
            )

            participant2.ledger_api.updates
              .topology_transactions(
                completeAfter = PositiveInt.one,
                partyIds = Seq(owner),
                beginOffsetExclusive = ledgerOffsetBeforePartyOnboarding,
                synchronizerFilter = Some(daId),
              )
              .loneElement
              .topologyTransaction
              .events
              .loneElement
              .event shouldBe TopologyEvent.Event.ParticipantAuthorizationAdded(
              ParticipantAuthorizationAdded(
                partyId = owner.toProtoPrimitive,
                participantId = participant3.uid.toProtoPrimitive,
                participantPermission = PARTICIPANT_PERMISSION_SUBMISSION,
              )
            )

            val onboardingTx = participant2.topology.party_to_participant_mappings
              .list(
                synchronizerId = daId,
                filterParty = owner.filterString,
                filterParticipant = participant3.filterString,
              )
              .loneElement
              .context

            File.usingTemporaryFile() { file =>
              participant2.parties.export_acs_at_timestamp(
                parties = Set(owner),
                synchronizerId = daId,
                topologyTransactionEffectiveTime = onboardingTx.validFrom,
                exportFilePath = file.toString,
              )
              val acs = tryLoadFrom(file)

              acs should have length 1
            }
        }
    }

    "fail to export ACS at a timestamp which does not correspond to a topology transaction (effective time)" in {
      implicit env =>
        import env.*
        WithEnabledParties(participant1 -> Seq("payer"), participant2 -> Seq("owner")) {
          case Seq(payer, owner) =>
            IouSyntax.createIou(participant1)(payer, owner)

            File.usingTemporaryFile() { file =>
              loggerFactory.assertThrowsAndLogs[CommandFailure](
                participant1.parties
                  .export_acs_at_timestamp(
                    parties = Set(payer),
                    exportFilePath = file.toString,
                    synchronizerId = daId,
                    topologyTransactionEffectiveTime = Instant.now(),
                  ),
                _.errorMessage should include(
                  PartyManagementServiceError.InvalidAcsSnapshotTimestamp.id
                ),
              )
              file.exists shouldBe false // file should be deleted if error
            }
        }
    }
  }

}
