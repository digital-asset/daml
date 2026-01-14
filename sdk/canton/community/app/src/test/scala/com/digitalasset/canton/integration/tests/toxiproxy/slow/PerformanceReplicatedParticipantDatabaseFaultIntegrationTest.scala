// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.config
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToPostgres,
  ProxyConfig,
  RunningProxy,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{
  UseConfigTransforms,
  UseExternalProcess,
  UseHAProxy,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.ReplicatedParticipantTestSetup
import com.digitalasset.canton.integration.tests.crashrecovery.LoadBalancedHAParticipantIntegrationTestBase
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.reliability.ReliabilityPerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import eu.rekawek.toxiproxy.model.ToxicDirection
import monocle.macros.syntax.lens.*
import org.scalatest.matchers.Matcher

import scala.concurrent.duration.*

/** Run a replicated participant under load with the performance runner while introducing database
  * connection faults on the active replica.
  *
  * Setup:
  *   - p1 and p4 form a replicated participant with a shared database
  *   - one toxiproxy per replica to the shared database
  *   - load balancer in front of the replicated participant ledger API to which the performance
  *     runner connect
  */
// TODO(i16601): this is known to be flaky
@UnstableTest
class PerformanceReplicatedParticipantDatabaseFaultIntegrationTest
    extends ReliabilityPerformanceIntegrationTest
    with BasePerformanceIntegrationTest
    with ReplicatedParticipantTestSetup
    with LoadBalancedHAParticipantIntegrationTestBase {

  // The performance runner base test runs against participant1 and 2, therefore we use p4 for the participant replica
  override protected lazy val passiveParticipantName = "participant4"

  // Combine the environment definition to setup replicas, health checks, and the performance runner
  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition
      .addConfigTransforms(ConfigTransforms.addMonitoringEndpointAllNodes*)
      .addConfigTransform(addRemoteLoadBalancedParticipant)
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.shutdownProcessing)
          .replace(config.NonNegativeDuration.tryFromDuration(1.minute))
      )
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.databaseConnectionTimeout)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(3))
        )
      )
      .withManualStart
      .withSetup { implicit env =>
        import env.*

        setupReplicas(env, Some(external))

        participant2.start()
        baseSetup
        performanceTestSetup
      }
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

  lazy val replica1ProxyConf: ProxyConfig =
    ParticipantToPostgres(
      s"$activeParticipantName-to-postgres",
      activeParticipantName,
      dbTimeouts = 1000L,
    )
  lazy val replica2ProxyConf: ProxyConfig =
    ParticipantToPostgres(
      s"$passiveParticipantName-to-postgres",
      passiveParticipantName,
      dbTimeouts = 1000L,
    )

  lazy val toxiProxyConf: UseToxiproxy.ToxiproxyConfig =
    UseToxiproxy.ToxiproxyConfig(List(replica1ProxyConf, replica2ProxyConf))
  lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)

  lazy val replica1Proxy: RunningProxy =
    toxiproxyPlugin.runningToxiproxy.getProxy(replica1ProxyConf.name).valueOrFail("replica1 proxy")
  lazy val replica2Proxy: RunningProxy =
    toxiproxyPlugin.runningToxiproxy.getProxy(replica2ProxyConf.name).valueOrFail("replica2 proxy")

  protected val external =
    new UseExternalProcess(
      loggerFactory,
      externalParticipants = Set(activeParticipantName, passiveParticipantName),
      fileNameHint = this.getClass.getSimpleName,
    )

  // Because the health check is currently on a per-process level, we need to use the external plugin to run the participant replicas
  lazy val haProxyPlugin =
    new UseHAProxy(
      mkLoadBalancerSetup(
        external,
        activeParticipantName,
        passiveParticipantName,
        "ledger-api",
      ),
      setRemoteParticipantToLoadBalancedLedgerApi,
      loggerFactory,
    )

  setupPlugins(
    new UsePostgres(loggerFactory),
    Seq(
      new UseConfigTransforms(Seq(ConfigTransforms.setStorageQueueSize(10000)), loggerFactory),
      toxiproxyPlugin,
      external,
      haProxyPlugin,
    ),
  )

  "introduce database connection faults on the participants while the runners are running" in {
    implicit env =>
      import env.*

      // Run the performance runner against the load balanced participant ledger API
      runWithFault(
        loadBalancedParticipantName,
        loadBalancedParticipant.config.ledgerApi.port,
        totalCycles = 50,
        numAssetsPerIssuer = 10,
      ) { hasCompleted =>
        def failDbConnection(done: Boolean): Unit =
          if (!done) {

            val replica1 = p(activeParticipantName)
            val replica2 = p(passiveParticipantName)

            val (activeReplica, passiveReplica) = getActiveAndPassive(replica1, replica2)

            val activeProxy =
              if (isActive(replica1)) replica1Proxy
              else if (isActive(replica2)) replica2Proxy
              else fail(s"None of the replica is active")

            logger.info(s"Introducing database connection fault on ${activeReplica.name}")

            // Slow down and terminate the connection
            val toxic = activeProxy.underlying
              .toxics()
              .timeout("db-con-timeout", ToxicDirection.UPSTREAM, 1000)

            waitPassive(activeReplica)
            waitActive(passiveReplica)

            logger.info(s"Removing database connection fault on ${activeReplica.name}")
            toxic.remove()
          }

        (1 to 10).foreach(_ => failDbConnection(hasCompleted()))
      }
      replica1Proxy.controllingToxiproxyClient.reset()
  }

  override protected def matchAcceptableErrorOrWarnMessage: Matcher[String] =
    super.matchAcceptableErrorOrWarnMessage
      .or {
        include(
          "Subscription failed with grpc-error: INTERNAL / Received unexpected EOS on empty DATA frame from server"
        )
      }
      .or(include("Completion Stream failed with an error"))
      .or(include("Ledger subscription PerformanceRunner failed with an error"))

}
