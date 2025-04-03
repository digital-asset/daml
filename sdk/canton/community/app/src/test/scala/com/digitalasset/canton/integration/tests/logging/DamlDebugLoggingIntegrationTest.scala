// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.logging

import com.daml.ledger.javaapi.data.codegen.{Exercised, Update}
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.damltests.java.damldebug.DebugTest
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import scala.jdk.CollectionConverters.*

class DamlDebugLoggingIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.submissionPhaseLogging.logLevel)
            .replace(Level.WARN) // use warn so we can assert
            .focus(_.parameters.engine.validationPhaseLogging.logLevel)
            .replace(
              Level.ERROR
            ) // use error so we can distinguish between submission and validation
            .focus(_.parameters.engine.validationPhaseLogging.enabled)
            .replace(true)
        )
      )
      .addConfigTransform(
        ConfigTransforms.updateParticipantConfig("participant1")(
          // test our wildcard matching
          _.focus(_.parameters.engine.submissionPhaseLogging.matching)
            .replace(Seq("Wont match", ".*IS ? D??L.*"))
        )
      )
      .addConfigTransform(
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.engine.submissionPhaseLogging.matching)
            .replace(Seq("Wont match at all"))
            .focus(_.parameters.engine.validationPhaseLogging.matching)
            .replace(Seq("Wont .*match ?t all"))
        )
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.foreach { participant =>
          participant.dars.upload(CantonTestsPath)
          participant.synchronizers.connect_local(sequencer1, alias = daName)
          val alice =
            participant.parties.enable("Alice")
          participant.ledger_api.javaapi.commands
            .submit(Seq(alice), DebugTest.create(alice.toLf).commands().asScala.toSeq)
        }

      }
  private val expected = "THIS IS A DAML LANGUAGE DEBUG MESSAGE"

  private def runAndCheck[T](
      participant: ParticipantReference
  )(contract: DebugTest.Contract => Update[Exercised[T]]): Assertion = {

    val alice = participant.parties.hosted("Alice").loneElement.party
    val cid = participant.ledger_api.javaapi.state.acs.await(DebugTest.COMPANION)(alice)

    loggerFactory.assertLogs(
      participant.ledger_api.javaapi.commands
        .submit(Seq(alice), contract(cid).commands().asScala.toSeq),
      _.warningMessage should include(expected),
      _.errorMessage should include(expected),
    )
    succeed

  }

  "the engine logger " should {
    "log debug" in { implicit env =>
      import env.*
      clue("log debug") {
        runAndCheck(participant1)(_.id.exerciseDebug(expected))
      }

    }
    "log debugRaw" in { implicit env =>
      import env.*
      clue("log debugRaw") {
        runAndCheck(participant1)(_.id.exerciseDebugRaw(expected))
      }
    }
    "log trace" in { implicit env =>
      import env.*
      clue("log trace") {
        runAndCheck(participant1)(_.id.exerciseTrace(expected))
      }
    }
    "log traceRaw" in { implicit env =>
      import env.*
      clue("log trace raw") {
        runAndCheck(participant1)(_.id.exerciseTraceRaw(expected))
      }
    }
    "log traceId" in { implicit env =>
      import env.*
      clue("log trace raw") {
        runAndCheck(participant1)(_.id.exerciseTraceId(expected))
      }
    }
    "not log if modules mismatches" in { implicit env =>
      import env.*
      val alice = participant2.parties.hosted("Alice").loneElement.party
      val cid = participant2.ledger_api.javaapi.state.acs.await(DebugTest.COMPANION)(alice)

      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        participant2.ledger_api.javaapi.commands
          .submit(Seq(alice), cid.id.exerciseDebug(expected).commands().asScala.toSeq),
        logs => {
          logs shouldBe empty
        },
      )
    }
  }

}
