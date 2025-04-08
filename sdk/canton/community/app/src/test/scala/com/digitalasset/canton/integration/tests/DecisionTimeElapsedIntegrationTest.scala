// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping
import com.digitalasset.canton.protocol.LocalRejectError
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*

import java.time.Duration
import java.util.UUID
import scala.jdk.CollectionConverters.*

trait DecisionTimeElapsedIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private lazy val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)
  private lazy val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { env =>
        import env.*
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            confirmationResponseTimeout = confirmationResponseTimeout.toConfig,
            mediatorReactionTimeout = mediatorReactionTimeout.toConfig,
          ),
        )
      }

  "mediator messages are delayed until after timeout" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant2.synchronizers.connect_local(sequencer1, daName)

    participant1.testing.state_inspection
      .lookupCleanTimeOfRequest(daName)
      .value
      .futureValueUS shouldBe None
    participant2.testing.state_inspection
      .lookupCleanTimeOfRequest(daName)
      .value
      .futureValueUS shouldBe None

    val sequencer = getProgrammableSequencer(sequencer1.name)

    val decisionTimeout = mediatorReactionTimeout.toScala.plus(confirmationResponseTimeout.toScala)

    // we delay sequencing the result until after the decision-time has elapsed
    // as the max-sequencing-time should be set to the decision-time this will cause the sequencer to drop the send
    // however the time-proofs should cause the transaction to timeout at the participant
    sequencer.setPolicy_("advance sim clock to after mediator timeout") {
      (submissionRequest: SubmissionRequest) =>
        submissionRequest.sender match {
          case _: MediatorId =>
            env.environment.simClock.value
              .advance(Duration.ofMillis(decisionTimeout.toMillis).plusSeconds(1))
            SendDecision.Process
          case _: ParticipantId | _: SequencerId =>
            SendDecision.Process
        }
    }

    val pingCommand =
      new Ping(
        UUID.randomUUID().toString,
        participant1.id.adminParty.toLf,
        participant2.id.adminParty.toLf,
      ).create.commands.asScala.toSeq

    loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
      participant1.ledger_api.javaapi.commands
        .submit(Seq(participant1.id.adminParty), pingCommand),
      // the decision time will be used for the max sequencing time, so the result message won't be sequenced in this test
      // the mediator will see that its send timed out and log a warning
      _.warningMessage should (include("Sequencing result message timed out.")),
      // the participants will see this as well and emit a timeout error (so 2 of such should be expected)
      _.shouldBeCantonError(
        LocalRejectError.TimeRejects.LocalTimeout,
        _ should startWith("Rejected transaction due to a participant determined timeout"),
      ),
      _.shouldBeCantonError(
        LocalRejectError.TimeRejects.LocalTimeout,
        _ should startWith("Rejected transaction due to a participant determined timeout"),
      ),
      // finally, the command failed in the console
      _.commandFailureMessage should include(
        "Rejected transaction due to a participant determined timeout"
      ),
    )
  }

  "rejection is observed on both participants, such as RequestIndex-es are moving ahead (regression test case for discovered MDEL-Indexer-Fusion bug)" in {
    implicit env =>
      import env.*

      eventually() {
        participant1.testing.state_inspection
          .lookupCleanTimeOfRequest(daName)
          .value
          .futureValueUS
          .value
          .rc
          .unwrap shouldBe 0L
        participant2.testing.state_inspection
          .lookupCleanTimeOfRequest(daName)
          .value
          .futureValueUS
          .value
          .rc
          .unwrap shouldBe 0L
      }
  }
}

class DecisionTimeElapsedIntegrationTestPostgres extends DecisionTimeElapsedIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
