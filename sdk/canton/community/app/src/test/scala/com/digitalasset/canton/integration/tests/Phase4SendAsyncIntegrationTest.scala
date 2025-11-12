// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Future, Promise}

/** This test ensures that phase 4 does not wait for sending the confirmation response.
  *
  * This is achieved by:
  *
  *   - Switching the number of maximum in-flight event batches to one (so that an incomplete phase
  *     4 blocks further processing)
  *   - Blocking the confirmation response of a request
  *   - Checking that a subsequent request can complete
  */
sealed trait Phase4SendAsyncIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.addConfigTransforms(
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.sequencerClient.maximumInFlightEventBatches).replace(PositiveInt.one)
      )
    )

  "Phase 4 should not wait for the send of the confirmation response" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant1.dars.upload(CantonExamplesPath)

    def getAcsSize(): Int =
      participant1.underlying.value.sync.stateInspection.contractCount.futureValueUS

    val party = participant1.adminParty

    val firstConfirmationResponseDelayed = new AtomicBoolean(false)
    val holdBackFirstConfirmationResponse = Promise[Unit]()

    getAcsSize() shouldBe 0

    getProgrammableSequencer(sequencer1.name).setPolicy_("hold first confirmation response") {
      submissionRequest =>
        // Delay first confirmation response
        if (
          ProgrammableSequencerPolicies.isConfirmationResponse(
            submissionRequest
          ) && firstConfirmationResponseDelayed.compareAndSet(false, true)
        ) {
          SendDecision.HoldBack(holdBackFirstConfirmationResponse.future)
        }
        /*
      We want to limit the messages that p1 has to process so we drop time proofs.
      There are no ACS commitments since there is a single participant.
         */
        else if (TimeProof.isTimeProofSubmission(submissionRequest)) SendDecision.Drop
        else SendDecision.Process
    }

    val create1F = Future(IouSyntax.createIou(participant1)(party, party))

    eventually() {
      firstConfirmationResponseDelayed.get() shouldBe true
    }

    val create2F = Future(IouSyntax.createIou(participant1)(party, party))
    eventually() {

      /** Because of the record order publisher, the create will not be visible on the ledger api
        * until the first creation succeeds. Hence, we rely on inspection of Canton contract store.
        */
      getAcsSize() shouldBe 1
    }

    /** Allow the first confirmation to go through to complete the first request and avoid shutdown
      * issues.
      */
    holdBackFirstConfirmationResponse.trySuccess(())

    create1F.futureValue
    create2F.futureValue
  }
}

class Phase4SendAsyncReferenceIntegrationTestPostgres extends Phase4SendAsyncIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
