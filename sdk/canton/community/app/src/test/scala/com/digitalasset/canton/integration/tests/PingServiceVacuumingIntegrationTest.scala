// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
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
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

class PingServiceVacuumingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.adminWorkflow.pingResponseTimeout)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(10))
        )
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        // warm up ping service
        participants.all.foreach(p => p.health.ping(p))
      }

  private def holdMessagesByP2(implicit env: TestConsoleEnvironment): Promise[Unit] = {
    import env.*
    val sequencer = getProgrammableSequencer(sequencer1.name)
    val flushPromise = Promise[Unit]()
    val p2id = participant2.id
    sequencer.setPolicy_("drop all messages from participant2") { submissionRequest =>
      if (submissionRequest.sender == p2id) {
        SendDecision.HoldBack(flushPromise.future)
      } else {
        SendDecision.Process
      }
    }
    flushPromise
  }

  private def resetPolicy(promise: Promise[Unit])(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    // flush promise such that the vacuuming submission requests of P2 don't timeout
    // as otherwise, they produce a warning in the logs
    promise.success(())
    getProgrammableSequencer(sequencer1.name).resetPolicy()
  }

  "PingService" should {
    "vacuum its own leftover ping contracts" in { implicit env =>
      import env.*

      val holdPromise = holdMessagesByP2(env)
      // Run 10 pings in parallel
      val nbPings = 10
      Future
        .traverse(1 to nbPings)(i =>
          Future {
            participant1.health
              .maybe_ping(
                participant2,
                timeout =
                  2000.milliseconds, // if this is too short, we'll abort the invocation and the ping won't happen
                id = s"command #$i",
              )
          }
        )
        .futureValue shouldBe Seq.fill(nbPings)(None)

      // All Ping contracts should eventually be gone, as they are automatically vacuumed
      // We can not wait for the ping to appear first as the vacuuming is quite aggressive
      eventually(timeUntilSuccess = 30.seconds) {
        val acs = participant1.ledger_api.state.acs
          .of_party(
            participant1.adminParty,
            limit = PositiveInt.tryCreate(nbPings * 2),
          )

        logger.debug(s"ACS for participant1 on synchronizer da contains ${acs.size} pings")
        acs shouldBe empty
      }
      resetPolicy(holdPromise)
    }

    "vacuum its own leftover ping proposal contracts" in { implicit env =>
      import env.*

      val holdPromise = holdMessagesByP2(env)

      participant1.testing.maybe_bong(
        targets = Set(participant2.id),
        validators = Set(participant2.id),
        timeout = 2000.milliseconds,
        id = "command",
      ) shouldBe None

      // The BongProposal contract should eventually be gone
      eventually() {
        participant1.ledger_api.state.acs.of_party(
          participant1.adminParty,
          limit = PositiveInt.two,
        ) shouldBe empty
      }

      resetPolicy(holdPromise)
    }

    "vacuum its own leftover ping contracts even when not single signatory" in { implicit env =>
      import env.*

      val holdPromise = holdMessagesByP2(env)

      // This test bong cleanup:
      participant1.testing.maybe_bong(
        targets = Set(participant2.id),
        validators = Set(participant3.id),
        levels = 1,
        timeout = 1000.milliseconds,
        id = s"command",
      ) shouldBe None

      // The bong contracts should eventually be gone
      eventually() {
        participant1.ledger_api.state.acs.of_party(
          participant1.adminParty,
          limit = PositiveInt.two,
        ) shouldBe empty
      }
      resetPolicy(holdPromise)
    }

    "vacuum contracts after restart" in { implicit env =>
      import env.*

      val holdPromise = holdMessagesByP2(env)

      val pingF = Future {
        participant1.health.maybe_ping(
          participant2.id,
          timeout = 7000.milliseconds,
        )
      }

      // Wait until we see the Ping contract
      eventually() {
        participant1.ledger_api.state.acs.of_party(
          participant1.adminParty,
          limit = PositiveInt.two,
          filterTemplates = TemplateId.templateIdsFromJava(Ping.TEMPLATE_ID),
        ) should not be empty
      }

      // restart p1
      clue("restarting p1") {
        participant1.stop()
        participant1.start()
        participant1.synchronizers.reconnect_all()
      }

      clue("waiting for vacuuming after restart") {
        // wait until ping got vacuumed
        eventually() {
          participant1.ledger_api.state.acs.of_party(
            participant1.adminParty,
            limit = PositiveInt.two,
          ) shouldBe empty
        }
      }

      resetPolicy(holdPromise)

      pingF.futureValue shouldBe None

      // works again nicely
      clue("flushing the system with a ping") {
        participant1.health.ping(participant2)
      }

    }

  }

}
