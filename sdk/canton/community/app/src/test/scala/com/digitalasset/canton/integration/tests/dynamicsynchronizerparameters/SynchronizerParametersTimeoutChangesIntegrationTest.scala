// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.dynamicsynchronizerparameters

import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.LocalRejectError.TimeRejects.LocalTimeout
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  ProgrammableSequencerPolicies,
}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level.WARN

import scala.concurrent.duration.DurationInt

trait SynchronizerParametersTimeoutChangesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private lazy val defaultsParameters = TestSynchronizerParameters.defaultDynamic

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        // Disable retries in the ping service so that any submission error is reported reliably
        // This makes the log messages more deterministic.
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.adminWorkflow.retries)
            .replace(false)
            // Increase the ping response time so that we don't get vacuuming interference
            .focus(_.parameters.adminWorkflow.pingResponseTimeout)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(240))
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)

        sequencer = getProgrammableSequencer(sequencer1.name)

        /*
           In order to simulate delays, we advance the clock when sequencing messages
           from the mediator and confirmation responses from participant1.

           The delays are the maximum ones such that a ping succeeds when the dynamic
           synchronizer parameters are the default ones.
         */
        sequencer.setPolicy("maximum delay policy")(
          ProgrammableSequencerPolicies.delay(environment = environment)(
            confirmationResponses =
              Map(participant1.id -> defaultsParameters.confirmationResponseTimeout),
            mediatorMessages = Some(defaultsParameters.mediatorReactionTimeout),
          )
        )
      }

  protected var sequencer: ProgrammableSequencer = _

  "A ping between two participants" should {
    "Succeed when delays at participant and mediator do not exceed maximum values" in {
      implicit env =>
        import env.*

        // wait until packages have been vetted
        Seq(participant1, participant2).foreach(_.packages.synchronize_vetting())

        assertPingSucceeds(participant1, participant2, timeoutMillis = 300000)
    }

    "Fail when delay at participant exceeds maximum value" in { implicit env =>
      import env.*

      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        _.update(
          confirmationResponseTimeout =
            defaultsParameters.confirmationResponseTimeout.toConfig.minusSeconds(1),
          // Need to increase mediatorReactionTimeout accordingly, so that mediator stays within the deadline.
          mediatorReactionTimeout =
            defaultsParameters.mediatorReactionTimeout.toConfig.plusSeconds(1),
        ),
        mustFullyAuthorize = true,
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        participant1.health.maybe_ping(participant2.id, timeout = 600.seconds) shouldBe None,
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.warningMessage should include regex "Response message for request .* timed out",
              "participant timeout",
            )
          ),
          mayContain = Seq(
            // MediatorError.Timeout is optional as sometimes the command service tracker times out first
            _.shouldBeCantonErrorCode(MediatorError.Timeout),
            _.warningMessage should include regex (
              "has exceeded the max-sequencing-time .* of the send request"
            ),
            _.warningMessage shouldBe "Sequencing result message timed out.",
          ),
        ),
      )
    }

    "Fail when delay at mediator exceeds maximum value" in { implicit env =>
      import env.*

      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        _.update(
          confirmationResponseTimeout = defaultsParameters.confirmationResponseTimeout.toConfig,
          mediatorReactionTimeout =
            defaultsParameters.mediatorReactionTimeout.toConfig.minusSeconds(1),
        ),
        mustFullyAuthorize = true,
      )

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
        clue("doing a ping") {
          participant1.health.maybe_ping(participant2.id, timeout = 600.seconds) shouldBe None
        },
        LogEntry.assertLogSeq(
          Seq(
            (_.warningMessage shouldBe "Sequencing result message timed out.", "mediator timeout"),
            (
              entry => {
                entry.shouldBeCantonErrorCode(LocalTimeout)
                entry.mdc.getOrElse("participant", "") shouldBe participant1.name
              },
              "participant1 local timeout",
            ),
            (
              entry => {
                entry.shouldBeCantonErrorCode(LocalTimeout)
                entry.mdc.getOrElse("participant", "") shouldBe participant2.name
              },
              "participant2 local timeout",
            ),
            (
              entry => {
                entry.shouldBeCantonErrorCode(LocalTimeout)
                entry.warningMessage should include regex ("Failed to submit ping.*due to a participant determined timeout")
              },
              "Ping service timeout",
            ),
          ),
          Seq(
            _.warningMessage should include regex (
              "has exceeded the max-sequencing-time .* of the send request"
            )
          ),
        ),
      )
    }
  }
}

class SynchronizerParametersTimeoutChangesReferenceIntegrationTestDefault
    extends SynchronizerParametersTimeoutChangesIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
