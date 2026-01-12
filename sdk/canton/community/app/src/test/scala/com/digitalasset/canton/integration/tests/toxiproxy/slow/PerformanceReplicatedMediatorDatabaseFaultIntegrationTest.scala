// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.config
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  MediatorToPostgres,
  ProxyConfig,
  RunningProxy,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.tests.ReplicatedNodeHelper
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.reliability.ReliabilityPerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import eu.rekawek.toxiproxy.model.ToxicDirection
import monocle.macros.syntax.lens.*
import org.scalatest.matchers.Matcher

import scala.concurrent.duration.*

/** Run a replicated mediator under load with the performance runner while introducing database
  * connection faults on the active replica.
  *
  * Setup:
  *   - m1 and m2 form a replicated mediator with a shared database (external processes)
  *   - one toxiproxy per replica to the shared database
  *   - p1 and sequencer1 are in process
  *   - above synchronizer is bootstrapped and p1 is connected to it
  *   - performance runner is run against p1
  */
class PerformanceReplicatedMediatorDatabaseFaultIntegrationTest
    extends ReliabilityPerformanceIntegrationTest
    with ReplicatedNodeHelper
    with BasePerformanceIntegrationTest {

  private val mediator1Name = "mediator1"
  private val mediator2Name = "mediator2"

  private def setupReplicas(
      env: TestConsoleEnvironment,
      external: UseExternalProcess,
  ): Unit = {
    import env.*

    external.start(mediator1Name)
    external.start(mediator2Name)

    Seq(rm(mediator1Name), rm(mediator2Name)).foreach(_.health.wait_for_running())
    val activeMediator =
      waitUntilOneActive(rm(mediator1Name), rm(mediator2Name), allowNonInit = true)

    new NetworkBootstrapper(
      NetworkTopologyDescription(
        daName,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(activeMediator),
      )(env)
    )(env).bootstrap()

    participant1.start()
    sequencer1.health.wait_for_initialized()
    activeMediator.health.wait_for_initialized()

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.health.wait_for_initialized()

    participant1.dars.upload(PerformanceTestPath)
    participant1.topology.synchronisation.await_idle()
  }

  // Combine the environment definition to setup replicas, health checks, and the performance runner
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 1,
        numSequencers = 1,
        numMediators = 2,
      )
      .addConfigTransforms(
        _.focus(_.parameters.timeouts.processing.shutdownProcessing)
          .replace(NonNegativeDuration.tryFromDuration(1.minute)),
        // Disable warnings about consistency checks as this test creates a lot of contracts
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.activationFrequencyForWarnAboutConsistencyChecks)
            .replace(Long.MaxValue)
        ),
      )
      .withManualStart
      .withSetup(setupReplicas(_, external))
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

  lazy val replica1ProxyConf: ProxyConfig =
    MediatorToPostgres(s"$mediator1Name-to-postgres", mediator1Name)
  lazy val replica2ProxyConf: ProxyConfig =
    MediatorToPostgres(s"$mediator2Name-to-postgres", mediator2Name)

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
      externalMediators = Set(mediator1Name, mediator2Name),
      fileNameHint = this.getClass.getSimpleName,
    )

  // Register plugins
  Seq(
    new UsePostgres(loggerFactory),
    UseSharedStorage.forMediators(
      mediator1Name,
      Seq(mediator2Name),
      loggerFactory,
    ),
    new UseConfigTransforms(
      Seq(
        ConfigTransforms.setStorageQueueSize(10000),
        ConfigTransforms.enableReplicatedMediators(),
        ConfigTransforms.setPassiveCheckPeriodMediators(config.PositiveFiniteDuration.ofSeconds(3)),
      ),
      loggerFactory,
    ),
    toxiproxyPlugin,
    external,
    new UseBftSequencer(loggerFactory),
  ).foreach(registerPlugin)

  "introduce database connection faults on the mediators while the runners are running" in {
    implicit env =>
      import env.*

      runWithFault(participant1.name, participant1.config.ledgerApi.port) { hasCompleted =>
        def failDbConnection(done: Boolean): Unit =
          if (!done) {

            val replica1 = rm(mediator1Name)
            val replica2 = rm(mediator2Name)

            val (activeReplica, passiveReplica) = getActiveAndPassive(replica1, replica2)

            val activeProxy =
              if (isActive(replica1)) replica1Proxy
              else if (isActive(replica2)) replica2Proxy
              else fail(s"None of the replica is active")

            // Slow down and terminate the connection
            val toxic = activeProxy.underlying
              .toxics()
              .timeout("db-con-timeout", ToxicDirection.UPSTREAM, 1000)

            waitPassive(activeReplica)
            waitActive(passiveReplica)

            toxic.remove()
          }

        (1 to 10).foreach(_ => failDbConnection(hasCompleted()))
      }

  }

  override protected def matchAcceptableErrorOrWarnMessage: Matcher[String] = (
    super.matchAcceptableErrorOrWarnMessage.or {
      // Happens because during mediator failover the pending requests are lost, which means some participant do not get
      // a verdict
      (include("LOCAL_VERDICT_TIMEOUT") and include(
        "Rejected transaction due to a participant determined timeout"
      ))
    }
  )

}
