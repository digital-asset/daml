// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.java.cycle.Cycle
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.ParticipantsSelector
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.topology.ExternalParty
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyTransaction,
}
import io.grpc.Status

/** Test and demonstrates onboarding of a multi hosted external party
  */
sealed trait MultiHostingInteractiveSubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasCycleUtils {

  private var aliceE: ExternalParty = _

  override protected def epn(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    env.participant1
  private val cpns: ParticipantsSelector = env => Seq(env.participant1, env.participant2)
  private val opns: ParticipantsSelector = env => Seq(env.participant3)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters.propose_update(
            synchronizer.synchronizerId,
            // Lower the confirmation response timeout to observe quickly rejections due to confirming
            // participants failing to respond in time
            _.update(confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(3)),
          )
        )

        participants.all.dars.upload(CantonExamplesPath)
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      }
      .addConfigTransforms(enableInteractiveSubmissionTransforms*)

  "Interactive submission" should {
    "host parties on multiple participants with a threshold" in { implicit env =>
      import env.*
      val (onboardingTransactions, externalParty) =
        participant1.parties.external
          .onboarding_transactions(
            "Alice",
            confirming = Seq(participant2),
            observing = Seq(participant3),
            confirmationThreshold = PositiveInt.two,
          )
          .futureValueUS
          .value

      loadOnboardingTransactions(
        externalParty,
        confirming = participant1,
        synchronizerId = daId,
        onboardingTransactions,
        extraConfirming = Seq(participant2),
        observing = Seq(participant3),
      )

      aliceE = externalParty

      val newPTP = TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.two,
        mapping = PartyToParticipant
          .create(
            aliceE.partyId,
            threshold = PositiveInt.two,
            Seq(
              HostingParticipant(participant1, ParticipantPermission.Confirmation, false),
              HostingParticipant(participant2, ParticipantPermission.Confirmation, false),
              HostingParticipant(participant3, ParticipantPermission.Observation, false),
            ),
          )
          .value,
        protocolVersion = testedProtocolVersion,
      )

      eventually() {
        participants.all.forall(
          _.topology.party_to_participant_mappings
            .is_known(
              daId,
              aliceE,
              hostingParticipants = participants.all,
              threshold = Some(newPTP.mapping.threshold),
            )
        ) shouldBe true
      }
    }

    "create a contract and read it from all confirming and observing participants" in {
      implicit env =>
        val contractId =
          createCycleContract(epn, aliceE, "test-external-signing").id.contractId

        // Get the created event from all confirming and observing
        val events = (cpns(env) ++ opns(env)).map { pn =>
          pn.ledger_api.event_query
            .by_contract_id(contractId, Seq(aliceE.partyId))
            .getCreated
            .getCreatedEvent
            // Nullify the offset as it will be different for each participant
            .withOffset(0)
        }
        // They should all be the same
        events.distinct should have size 1
    }

    "fail if not enough confirming participants confirm" in { implicit env =>
      import env.*
      // Stop one of the 2 CPNs - threshold is 2 so the transaction cannot be committed
      participant2.stop()
      val prepared = ppn.ledger_api.interactive_submission
        .prepare(Seq(aliceE), Seq(createCycleCommand(aliceE, "test-external-signing")))

      val signatures = global_secret.sign(prepared.preparedTransactionHash, aliceE)
      val (submissionId, ledgerEnd) = exec(prepared, Map(aliceE.partyId -> signatures), epn)
      val completion = findCompletion(
        submissionId,
        ledgerEnd,
        aliceE,
        epn,
      )
      completion.status.value.code should not be Status.Code.OK.value()
      completion.status.value.message should include(MediatorError.Timeout.id)
    }

    var contractIdMissedByObserving: String = ""
    "observing participant are not needed to confirm" in { implicit env =>
      import env.*
      participant3.stop()
      participant2.start()
      participant2.synchronizers.reconnect_all()
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          contractIdMissedByObserving =
            createCycleContract(epn, aliceE, "test-external-signing").id.contractId
        },
        LogEntry.assertLogSeq(
          Seq.empty,
          // Logged by P2 as it re-connects and processes the now timed out confirmation request
          Seq(_.warningMessage should include("timed out")),
        ),
      )
    }

    "observing participant catch up after being brought back up" in { implicit env =>
      import env.*
      participant3.start()
      participant3.synchronizers.reconnect_all()

      eventually() {
        participant3.ledger_api.state.acs
          .active_contracts_of_party(
            aliceE.partyId,
            filterTemplates = Seq(TemplateId.fromJavaIdentifier(Cycle.TEMPLATE_ID)),
            includeCreatedEventBlob = true,
          )
          .flatMap(_.createdEvent)
          .filter(_.contractId == contractIdMissedByObserving)
          .loneElement
      }
    }
  }
}

final class MultiHostingInteractiveSubmissionIntegrationTestH2
    extends MultiHostingInteractiveSubmissionIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}
