// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.console.commands.PartiesAdministration
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.ExternalPartyAlreadyExists
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
}
import com.digitalasset.canton.topology.transaction.{HostingParticipant, PartyToParticipant}

import scala.concurrent.Future

trait ExternalPartyOnboardingIntegrationTestSetup
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasCycleUtils {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      }
      .addConfigTransforms(enableInteractiveSubmissionTransforms*)
}

class ExternalPartyOnboardingIntegrationTest extends ExternalPartyOnboardingIntegrationTestSetup {
  "External party onboarding" should {
    "host parties on multiple participants with a threshold" in { implicit env =>
      import env.*
      val (onboardingTransactions, externalParty) =
        participant1.parties.external
          .onboarding_transactions(
            "Alice",
            additionalConfirming = Seq(participant2),
            observing = Seq(participant3),
            confirmationThreshold = PositiveInt.two,
          )
          .futureValueUS
          .value

      Seq(participant1, participant2, participant3).map { hostingNode =>
        hostingNode.ledger_api.parties.allocate_external(
          synchronizer1Id,
          onboardingTransactions.transactionsWithSingleSignature,
          multiSignatures = onboardingTransactions.multiTransactionSignatures,
        )
      }

      PartiesAdministration.Allocation.waitForPartyKnown(
        partyId = externalParty.partyId,
        hostingParticipant = participant1,
        synchronizeParticipants = Seq(participant1, participant2, participant3),
        synchronizerId = synchronizer1Id.logical,
      )
    }

    "allocate a party from one of their observing nodes" in { implicit env =>
      import env.*

      val (onboardingTransactions, externalParty) = participant1.parties.external
        .onboarding_transactions(
          "Bob",
          observing = Seq(participant2),
        )
        .futureValueUS
        .value
      val partyId = externalParty.partyId

      participant2.ledger_api.parties.allocate_external(
        synchronizer1Id,
        onboardingTransactions.transactionsWithSingleSignature,
        onboardingTransactions.multiTransactionSignatures,
      )

      // Use the admin API to authorize the hosting in this test, but it can also be done via the
      // allocateExternalParty endpoint on the admin API
      // See multi hosted decentralized party below for an example
      val partyToParticipantProposal = eventually() {
        participant1.topology.party_to_participant_mappings
          .list(
            synchronizer1Id,
            proposals = true,
            filterParty = partyId.toProtoPrimitive,
          )
          .loneElement
      }
      val transactionHash = partyToParticipantProposal.context.transactionHash
      participant1.topology.transactions.authorize[PartyToParticipant](
        transactionHash,
        mustBeFullyAuthorized = false,
        store = TopologyStoreId.Synchronizer(synchronizer1Id),
      )

      PartiesAdministration.Allocation.waitForPartyKnown(
        partyId = externalParty.partyId,
        hostingParticipant = participant1,
        synchronizeParticipants = Seq(participant1, participant2),
        synchronizerId = synchronizer1Id.logical,
      )
    }

    "allocate a decentralized multi-hosted multi-sig external party" in { implicit env =>
      import env.*

      // Create the namespace owners first
      val namespace1 = participant1.parties.external.create_external_namespace()
      val namespace2 = participant1.parties.external.create_external_namespace()
      val namespace3 = participant1.parties.external.create_external_namespace()
      val namespaceOwners = NonEmpty.mk(Set, namespace1, namespace2, namespace3)

      val confirmationThreshold = PositiveInt.two
      val keysCount = PositiveInt.three
      val keysThreshold = PositiveInt.two
      val namespaceThreshold = PositiveInt.three

      // Generate the corresponding onboarding transactions
      val onboardingData = participant1.parties.external.onboarding_transactions(
        name = "Emily",
        additionalConfirming = Seq(participant2),
        observing = Seq(participant3),
        confirmationThreshold = confirmationThreshold,
        keysCount = keysCount,
        keysThreshold = keysThreshold,
        decentralizedNamespaceOwners = namespaceOwners.forgetNE,
        namespaceThreshold = namespaceThreshold,
      )

      val (onboardingTransactions, emilyE) = onboardingData.futureValueUS.value

      // Start by having the extra hosting nodes authorize the hosting
      // We can do that even before the party namespace is authorized
      Seq(participant2, participant3).map { hostingNode =>
        hostingNode.ledger_api.parties.allocate_external(
          synchronizer1Id,
          Seq(onboardingTransactions.partyToParticipant.transaction -> Seq.empty),
          multiSignatures = Seq.empty,
        )
      }

      // Then load all transactions via the allocate endpoint
      participant1.ledger_api.parties.allocate_external(
        synchronizer1Id,
        onboardingTransactions.transactionsWithSingleSignature,
        multiSignatures = onboardingTransactions.multiTransactionSignatures,
      )

      // Eventually everything should be authorized correctly
      eventually() {
        val p2p = participant1.topology.party_to_participant_mappings
          .list(filterParty = emilyE.partyId.filterString, synchronizerId = synchronizer1Id)

        p2p.loneElement.item.partyId shouldBe emilyE.partyId
        p2p.loneElement.item.threshold shouldBe confirmationThreshold
        p2p.loneElement.item.participants contains HostingParticipant(participant1, Confirmation)
        p2p.loneElement.item.participants contains HostingParticipant(participant2, Confirmation)
        p2p.loneElement.item.participants contains HostingParticipant(participant3, Observation)
      }

      eventually() {
        val p2k = participant1.topology.party_to_key_mappings.list(
          filterParty = emilyE.partyId.filterString,
          store = synchronizer1Id,
        )

        p2k.loneElement.item.party shouldBe emilyE.partyId
        p2k.loneElement.item.threshold shouldBe keysThreshold
        p2k.loneElement.item.signingKeys.forgetNE
          .map(_.fingerprint) should contain theSameElementsAs emilyE.signingFingerprints.forgetNE
      }

      eventually() {
        val dnd = participant1.topology.decentralized_namespaces.list(
          filterNamespace = emilyE.partyId.namespace.filterString,
          store = synchronizer1Id,
        )

        dnd.loneElement.item.namespace shouldBe emilyE.partyId.uid.namespace
        dnd.loneElement.item.threshold shouldBe namespaceThreshold
        dnd.loneElement.item.owners.forgetNE shouldBe namespaceOwners.forgetNE
      }
    }

    "provide useful error message when the participant is not connected to the synchronizer" in {
      implicit env =>
        import env.*
        val (onboardingTransactions, _) =
          participant1.parties.external.onboarding_transactions("Alice").futureValueUS.value

        participant1.synchronizers.disconnect_all()

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.parties.allocate_external(
            synchronizer1Id,
            onboardingTransactions.transactionsWithSingleSignature,
            onboardingTransactions.multiTransactionSignatures,
          ),
          _.errorMessage should include(
            s"This node is not connected to the requested synchronizer ${synchronizer1Id.logical}."
          ),
        )

        participant1.synchronizers.reconnect_all()
    }

    "provide useful error message when onboarding the same party twice" in { implicit env =>
      import env.*

      val (onboardingTransactions, partyE) =
        participant1.parties.external.onboarding_transactions("Alice").futureValueUS.value

      def allocate() =
        participant1.ledger_api.parties.allocate_external(
          synchronizer1Id,
          onboardingTransactions.transactionsWithSingleSignature,
          onboardingTransactions.multiTransactionSignatures,
        )

      // Allocate once
      allocate()
      participant1.ledger_api.parties.list().find(_.party == partyE.partyId) shouldBe defined

      // Allocate a second time
      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        allocate(),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include(
                ExternalPartyAlreadyExists.Failure(partyE.partyId, synchronizer1Id).cause
              ),
              "Expected party already exists error",
            )
          )
        ),
      )
    }

    "provide useful error message when concurrently retrying onboarding requests for the same party" in {
      implicit env =>
        import env.*

        val (onboardingTransactions, partyE) =
          participant1.parties.external.onboarding_transactions("Alice").futureValueUS.value

        def allocate() =
          participant1.ledger_api.parties.allocate_external(
            synchronizer1Id,
            onboardingTransactions.transactionsWithSingleSignature,
            onboardingTransactions.multiTransactionSignatures,
          )

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            val results = timeouts.default.await("Waiting for concurrent allocation attempts")(
              Seq
                .fill(10)(
                  Future(allocate()).map(_ => Right(())).recover { case ex => Left(ex) }
                )
                .sequence
            )
            // Only one of them should be a success
            results.count(_.isRight) shouldBe 1
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.errorMessage should include(
                  s"Party ${partyE.partyId.uid.identifier.str} is in the process of being allocated on this node."
                ),
                "Expected party already exists error",
              )
            ),
            // It's not impossible that one of the calls gets in late when the party is already fully allocated and
            // when no other call is in flight, so catch that case here
            Seq(
              _.errorMessage should include(
                ExternalPartyAlreadyExists.Failure(partyE.partyId, synchronizer1Id).cause
              )
            ),
          ),
        )

        // Check the party was still allocated
        participant1.ledger_api.parties.list().find(_.party == partyE.partyId) shouldBe defined
    }
  }
}
