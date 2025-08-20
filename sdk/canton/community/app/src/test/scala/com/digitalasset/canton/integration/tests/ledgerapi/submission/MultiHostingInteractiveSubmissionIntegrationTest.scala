// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.damltests.java.cycle.Cycle
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.{
  ParticipantSelector,
  ParticipantsSelector,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.interactive.ExternalPartyUtils.ExternalParty
import com.digitalasset.canton.logging.LogEntry
import io.grpc.Status

/** Test and demonstrates onboarding of a multi hosted external party
  */
trait MultiHostingInteractiveSubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest {

  private var aliceE: ExternalParty = _

  override protected val epn: ParticipantSelector = _.participant1
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
            _.update(confirmationResponseTimeout = NonNegativeFiniteDuration.ofSeconds(3)),
          )
        )
        Seq(participant1, participant2, participant3).foreach { p =>
          p.dars.upload(CantonExamplesPath)
          p.synchronizers.connect_local(sequencer1, alias = daName)
        }
      }
      .addConfigTransforms(enableInteractiveSubmissionTransforms*)

  "Interactive submission" should {
    "host parties on multiple participants with a threshold" in { implicit env =>
      import env.*
      aliceE = onboardParty(
        "Alice",
        confirming = participant1,
        env.synchronizer1Id,
        extraConfirming = Seq(participant2),
        observing = Seq(participant3),
        confirmationThreshold = PositiveInt.two,
      )
      waitForExternalPartyToBecomeEffective(
        aliceE,
        participant1,
        participant2,
        participant3,
        env.sequencer1,
      )
    }

    "create a contract and read it from all confirming and observing participants" in {
      implicit env =>
        val contractId = createCycleContract(aliceE).contractId
        // Get the created event from all confirming and observing
        val events = (cpns(env) ++ opns(env)).map { pn =>
          getCreatedEvent(contractId, aliceE.partyId, pn)
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
      val prepared = prepareCommand(aliceE, protoCreateCycleCmd(aliceE))
      val signatures = signTxAs(prepared, aliceE)
      val (submissionId, ledgerEnd) = exec(prepared, signatures, epn)
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
        { contractIdMissedByObserving = createCycleContract(aliceE).contractId },
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
        getCreatedEvent(
          contractIdMissedByObserving,
          aliceE.partyId,
          participant3,
          requireBlob = Some(TemplateId.fromJavaIdentifier(Cycle.TEMPLATE_ID)),
        )
      }
    }
  }

}

class MultiHostingInteractiveSubmissionIntegrationTestH2
    extends MultiHostingInteractiveSubmissionIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}
