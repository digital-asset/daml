// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast

import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.tag.Reliability.*
import com.digitalasset.canton.config.IdentityConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.*
import com.digitalasset.canton.integration.tests.topology.TopologyManagementHelper
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import eu.rekawek.toxiproxy.model.ToxicDirection
import eu.rekawek.toxiproxy.model.toxic.Timeout
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

trait ReplicatedParticipantBootstrapDatabaseFaultIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ReplicatedParticipantTestSetup
    with ReliabilityTestSuite
    with TopologyManagementHelper {

  def proxyConf: ProxyConfig

  lazy val toxiProxyConf = UseToxiproxy.ToxiproxyConfig(List(proxyConf))
  lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)

  protected val getToxiProxy: () => RunningProxy

  protected def setupPluginsWithToxiproxy(
      storagePlugin: EnvironmentSetupPlugin,
      blockSequencerPlugin: UseBftSequencer,
  ): () => RunningProxy = {
    registerPlugin(storagePlugin)
    registerPlugin(blockSequencerPlugin)
    registerPlugin(toxiproxyPlugin)
    () => toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition.withManualStart
      .addConfigTransforms(
        // Set auto init to false to force the bootstrap to stop before the node is initialized
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.init.identity)
            .replace(IdentityConfig.Manual)
            .focus(_.init.generateTopologyTransactionsAndKeys)
            .replace(false)
        ),
        ConfigTransforms.enableReplicatedParticipants(),
      )
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

}

class ParticipantBootstrapDatabaseFaultIntegrationTestPostgres
    extends ReplicatedParticipantBootstrapDatabaseFaultIntegrationTest
    with AccessTestScenario {

  "A replicated participant" must {
    "become passive if losing DB connection during bootstrap" in { implicit env =>
      import env.*

      participant1.start()
      participant1.health.wait_for_ready_for_id()

      var toxic: Option[Timeout] = None

      loggerFactory.assertEventuallyLogsSeq(LevelAndAbove(Level.INFO))(
        {
          toxic = Some(
            getToxiProxy().underlying
              .toxics()
              .timeout("db-con-timeout", ToxicDirection.UPSTREAM, 1000)
          )

        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.infoMessage should include(
                "Participant replica is becoming passive (participant services not initialized yet: nothing to do)"
              ),
              "skipped passive function",
            ),
            (
              _.infoMessage should include(
                "Successfully performed replica state change to Passive"
              ),
              "transition to passive message",
            ),
          ),
          Seq(_ => succeed),
        ),
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          toxic.foreach(_.remove())

          waitActive(participant1, allowNonInit = true)
          manuallyInitNode(participant1)
          waitActive(participant1, allowNonInit = false)
        },
        LogEntry.assertLogSeq(
          Seq.empty,
          Seq(
            // Depending on the storage health check timing, the passive -> active transition can run
            // at the same time as node initialization. This is supported, but it may log this warning because
            // the scheduler is being re-activated (once in the node initialize method and
            // once in the active transition method)
            _.warningMessage should include(
              "Scheduler unexpectedly already started. Stopping old executor"
            )
          ),
        ),
      )

    }
  }

  override def proxyConf: ProxyConfig =
    ParticipantToPostgres(s"p1-to-postgres", activeParticipantName)

  override protected val getToxiProxy: () => RunningProxy =
    setupPluginsWithToxiproxy(
      new UsePostgres(loggerFactory),
      new UseBftSequencer(loggerFactory),
    )
}
