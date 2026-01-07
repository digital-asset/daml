// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.console.{RemoteInstanceReference, RemoteMediatorReference}
import com.digitalasset.canton.integration.plugins.{UseExternalProcess, UsePostgres}
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.reliability.ReliabilityPerformanceIntegrationTest
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.TracedLogger
import monocle.macros.syntax.lens.*
import org.scalatest.matchers.Matcher

/** This class is in the crashrecovery (and not the reliability) packages because it uses the
  * external process plugin and currently using this plugin requires dumping the sbt classpath which
  * is only done in the crashrecovery tests.
  */
@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
class RestartMediatorReliabilityIntegrationTest
    extends ReliabilityPerformanceIntegrationTest
    with BasePerformanceIntegrationTest
    with MediatorFailoverIntegrationTest {

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition
      .addConfigTransform(
        // Disable warnings about consistency checks as this test creates a lot of contracts
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.activationFrequencyForWarnAboutConsistencyChecks)
            .replace(Long.MaxValue)
        )
      )
      .withSetup { implicit env =>
        performanceTestSetup
      }

  setupPluginsForMediator(new UsePostgres(loggerFactory), Seq(storageQueuePlugin))

  override protected def performanceTestSetup(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    participant2.parties.enable("0Cleese")

    // Deploying packages locally, because remote deployment would not wait for packages becoming ready.
    Seq(participant1, participant2).foreach {
      _.dars.upload(PerformanceTestPath)
    }
  }

  "kill a mediator while under load".taggedAs(
    ReliabilityTest(
      Component(
        "Participant",
        "connected to non-replicated sequencer that is connected to a replicated mediator",
      ),
      AdverseScenario(
        dependency = "Mediator node",
        details = "active mediator node process is forcefully stopped and restarted while running",
      ),
      Remediation(
        remediator = "passive mediators",
        action =
          "the passive mediators recognize that the active mediator is offline, one of them becomes active, acquiring " +
            "the mediator DB lock, and connects to the sequencer to receive future requests",
      ),
      outcome = "transaction processing continuously possible between fail-overs",
    )
  ) in { implicit env =>
    import env.*
    val mediator1Rem: RemoteMediatorReference = rm(mediator1Name)
    val mediator2Rem: RemoteMediatorReference = rm(mediator2Name)

    runWithFault(participant1.name, participant1.config.ledgerApi.port) { hasCompleted =>
      def remoteMediatorOutage(
          external: UseExternalProcess,
          mediator1: RemoteInstanceReference,
          mediator2: RemoteInstanceReference,
          hasCompleted: () => Boolean,
          logger: TracedLogger,
      )(index: Int): Unit =
        if (!hasCompleted()) {
          // alternate between nodes to take down
          val node = if (index % 2 == 0) mediator1 else mediator2

          logger.info(s"Killing ${node.name} (index=$index)")
          external.kill(node.name)
          logger.info(s"Starting ${node.name}")
          external.start(node.name)
          node.health.wait_for_running()
        } else {
          checkPerfRunnerCompletion(index)
        }
      (1 to 5).foreach(
        remoteMediatorOutage(externalPlugin, mediator1Rem, mediator2Rem, hasCompleted, logger)
      )
    }
  }

  override protected def matchAcceptableErrorOrWarnMessage: Matcher[String] = (
    include("LOCAL_VERDICT_TIMEOUT") or include("Submission timed out at")
      or // frequently happens during resource contention on CI when slick queue is full
      include regex ("The operation '.*store\\.db.*' has failed with an exception. Retrying ")
      or
      include regex ("Now retrying operation '.*store\\.db.*'")
      or super.matchAcceptableErrorOrWarnMessage
  )
}
