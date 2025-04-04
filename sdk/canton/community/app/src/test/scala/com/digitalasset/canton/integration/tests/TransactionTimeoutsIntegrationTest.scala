// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.completion.Completion
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Finality
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.error.{CantonBaseError, MediatorError}
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.protocol.LocalRejectError
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*

import java.time.Duration
import java.util.UUID
import scala.concurrent.Promise

abstract class TransactionTimeoutsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestSuite {

  val finality: SecurityTest = SecurityTest(property = Finality, asset = "participant")

  val confirmationResponseTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(3)
  val mediatorReactionTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfSeconds(3)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.update(
            confirmationResponseTimeout = confirmationResponseTimeout.toConfig,
            mediatorReactionTimeout = mediatorReactionTimeout.toConfig,
          ),
        )

        participant1.synchronizers.connect_local(sequencer1, daName)
        participant1.dars.upload(CantonExamplesPath)

      }

  def attemptCreateAndWait(
      sequencer: ProgrammableSequencer
  )(implicit env: TestConsoleEnvironment): Completion = {
    import env.*
    val uuid = UUID.randomUUID()
    val completion = clue(s"creating a cycle command $uuid") {
      val ledgerEnd = participant1.ledger_api.state.end()
      val cycle = new Cycle(
        uuid.toString,
        participant1.adminParty.toProtoPrimitive,
      ).create.commands.loneElement
      participant1.ledger_api.javaapi.commands
        .submit_async(Seq(participant1.adminParty), Seq(cycle))
      val completions =
        participant1.ledger_api.completions.list(participant1.adminParty, 1, ledgerEnd)
      val completion = completions.loneElement
      completion.status.value.code shouldNot be(com.google.rpc.Code.OK_VALUE)

      completion
    }

    // follow immediately with a ping to ensure the timeout message at the participant is triggered (otherwise it's flaky)
    sequencer.resetPolicy()
    logger.info(s"Starting a ping")
    val time = assertPingSucceeds(participant1, participant1)
    logger.info(s"Ping completed in $time")

    completion
  }

  "A participant can recover from a dropped mediator message (result message)" taggedAs finality
    .setAttack(
      attack = Attack(
        actor = "mediator operator",
        threat = "takes the mediator offline",
        mitigation = "timeout in-flight requests",
      )
    ) in { implicit env =>
    import env.*

    val sequencer = getProgrammableSequencer(sequencer1.name)

    sequencer.setPolicy_("drop mediator messages")(SendPolicy.processTimeProofs_ {
      submissionRequest =>
        submissionRequest.sender match {
          case _: MediatorId =>
            env.environment.simClock.value.advance(Duration.ofSeconds(10))
            SendDecision.Reject
          case _: ParticipantId | _: SequencerId => SendDecision.Process
        }
    })

    val completion = loggerFactory.assertLoggedWarningsAndErrorsSeq(
      attemptCreateAndWait(sequencer),
      LogEntry.assertLogSeq(
        Seq(
          (
            _.errorMessage should (include("Request failed for sequencer") and
              include("Message rejected by send policy.")),
            "Mediator send attempts",
          ),
          (
            _.shouldBeCantonError(
              LocalRejectError.TimeRejects.LocalTimeout,
              _ should startWith("Rejected transaction due to a participant determined timeout"),
            ),
            "timeout warning by the participant",
          ),
        ),
        Seq(_.warningMessage should include("Sequencing result message timed out")),
      ),
    )
    CantonBaseError.isStatusErrorCode(MediatorError.Timeout, completion.status.value)
  }

  "A participant can recover from a dropped participant message (confirmation response)" taggedAs finality
    .setAttack(
      attack = Attack(
        actor = "participant operator",
        threat = "takes a participant offline",
        mitigation = "timeout in-flight requests",
      )
    ) in { implicit env =>
    import env.*

    val sequencer = getProgrammableSequencer(sequencer1.name)
    val participant1Id = participant1.id
    val advanceClock = Promise[Unit]()

    // advance the clock such that the mediator will request a time proof when asked
    env.environment.simClock.value.advance(Duration.ofSeconds(5))

    sequencer.setPolicy_("drop participant response messages") { submissionRequest =>
      submissionRequest.sender match {
        // capture and drop the confirmation response
        case `participant1Id` if isConfirmationResponse(submissionRequest) =>
          advanceClock.success(())
          SendDecision.Drop
        case _other =>
          SendDecision.Process
      }
    }

    advanceClock.future.foreach { _ =>
      logger.debug("participant response is dropped, flushing the mediator")
      // ensure we flush the "mediator" message processing before advancing the clock.
      // if the mediator receives the original confirmation request after the clock is advanced,
      // it won't not schedule a clock event that would eventually lead to a time out
      // however, before we started the test, we advanced the clock. so the mediator doesn't know a current
      // time anymore. now, the following fetch synchronizer time will trigger a tick request if the
      // mediator has not yet processed the confirmation request. otherwise, it will just return the
      // time of the confirmation request.
      mediator1.testing.fetch_synchronizer_time()
      // now advance the clock
      env.environment.simClock.value
        .advance(confirmationResponseTimeout.unwrap.plus(Duration.ofSeconds(1)))
    }

    val completion = loggerFactory.assertLogsUnorderedOptional(
      attemptCreateAndWait(sequencer),
      LogEntryOptionality.Required -> (_.warningMessage should include regex
        "Response message for request \\[.*\\] timed out at"),
    )
    val status = completion.status.value
    CantonBaseError.isStatusErrorCode(MediatorError.Timeout, status)
    val error = DecodedCantonError.fromGrpcStatus(status).value
    val unresponsiveParties = error.context.get("unresponsiveParties")
    unresponsiveParties shouldBe Some(participant1.adminParty.toLf)
  }
}

class TransactionTimeoutsReferenceIntegrationTestPostgres
    extends TransactionTimeoutsIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
