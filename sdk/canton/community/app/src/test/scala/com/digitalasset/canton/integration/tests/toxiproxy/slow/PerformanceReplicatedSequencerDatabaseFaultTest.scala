// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import better.files.{File, Resource}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ProxyConfig,
  RunningProxy,
  SequencerToPostgres,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{
  UseConfigTransforms,
  UseExternalProcess,
  UsePostgres,
  UseSharedStorage,
}
import com.digitalasset.canton.integration.tests.ReplicatedNodeHelper
import com.digitalasset.canton.integration.tests.health.HealthMonitoringTestUtils
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.reliability.ReliabilityPerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import eu.rekawek.toxiproxy.model.ToxicDirection
import io.grpc.health.v1.health.HealthGrpc
import monocle.macros.syntax.lens.*
import org.scalatest.matchers.Matcher

import scala.concurrent.duration.*

/** Run 3 sequencers under load with the performance runner while introducing database connection
  * faults on the active replica.
  *
  * Setup:
  *   - 3 active sequencers as external processes
  *   - DB connection is cut in a round robin fashion to each of them
  *   - performance runner runs on participant1
  *   - participant1, mediator1 and dm1 are run in-process
  */
// TODO(#16089): Currently this test cannot work due to the issue
class PerformanceReplicatedSequencerDatabaseFaultTest
    extends ReliabilityPerformanceIntegrationTest
    with ReplicatedNodeHelper
    with HealthMonitoringTestUtils
    with BasePerformanceIntegrationTest {

  private val sequencer1Name = "sequencer1"
  private val sequencer2Name = "sequencer2"
  private val sequencer3Name = "sequencer3"

  private def setupReplicas(
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    external.start(sequencer1Name)
    external.start(sequencer2Name)
    external.start(sequencer3Name)
    mediator1.start()

    remoteSequencer1.health.wait_for_ready_for_initialization()
    remoteSequencer2.health.wait_for_ready_for_initialization()
    remoteSequencer3.health.wait_for_ready_for_initialization()
    mediator1.health.wait_for_ready_for_initialization()
  }

  private def connectParticipant(
      env: TestConsoleEnvironment
  ) = {
    import env.*

    participant1.start()
    participant1.synchronizers.connect_multi(
      daName,
      Seq(
        remoteSequencer1,
        remoteSequencer2,
        remoteSequencer3,
      ),
    )
    participant1.health.wait_for_initialized()
    participant1.dars.upload(
      this.getClass.getClassLoader.getResource("PerformanceTest.dar").getPath
    )
    participant1.topology.synchronisation.await_idle()
  }

  // Combine the environment definition to setup replicas, health checks, and the performance runner
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .fromFiles(
        // TODO(#15837) Kill the file multi-synchronizer-external-mediators.conf
        File(Resource.getUrl("multi-synchronizer-external-mediators.conf"))
      )
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.shutdownProcessing)
          .replace(config.NonNegativeDuration.tryFromDuration(1.minute))
      )
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.sequencerClient.defaultMaxSequencingTimeOffset)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(5))
        )
      )
      .addConfigTransforms(ConfigTransforms.addMonitoringEndpointForSequencers*)
      .withManualStart
      .withSetup(setupReplicas)
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            synchronizerAlias = daName,
            synchronizerOwners = Seq(remoteSequencer1, remoteSequencer2, remoteSequencer3),
            synchronizerThreshold = PositiveInt.two,
            sequencers = Seq(remoteSequencer1, remoteSequencer2, remoteSequencer3),
            mediators = Seq(mediator1),
          )
        )
      }
      .withSetup(connectParticipant)
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

  private lazy val replica1ProxyConf: ProxyConfig =
    SequencerToPostgres(s"$sequencer1Name-to-postgres", sequencer1Name)
  private lazy val replica2ProxyConf: ProxyConfig =
    SequencerToPostgres(s"$sequencer2Name-to-postgres", sequencer2Name)
  private lazy val replica3ProxyConf: ProxyConfig =
    SequencerToPostgres(s"$sequencer3Name-to-postgres", sequencer3Name)

  private lazy val toxiProxyConf: UseToxiproxy.ToxiproxyConfig =
    UseToxiproxy.ToxiproxyConfig(
      List(
        replica1ProxyConf,
        replica2ProxyConf,
        replica3ProxyConf,
      )
    )
  private lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)

  private lazy val replica1Proxy: RunningProxy =
    toxiproxyPlugin.runningToxiproxy.getProxy(replica1ProxyConf.name).valueOrFail("replica1 proxy")
  private lazy val replica2Proxy: RunningProxy =
    toxiproxyPlugin.runningToxiproxy.getProxy(replica2ProxyConf.name).valueOrFail("replica2 proxy")
  private lazy val replica3Proxy: RunningProxy =
    toxiproxyPlugin.runningToxiproxy.getProxy(replica3ProxyConf.name).valueOrFail("replica3 proxy")

  protected val external =
    new UseExternalProcess(
      loggerFactory,
      externalSequencers = Set(sequencer1Name, sequencer2Name, sequencer3Name),
      fileNameHint = this.getClass.getSimpleName,
    )

  // Register plugins
  Seq(
    new UsePostgres(loggerFactory),
    UseSharedStorage.forSequencers(
      sequencer1Name,
      Seq(sequencer2Name, sequencer3Name),
      loggerFactory,
    ),
    new UseConfigTransforms(
      Seq(
        ConfigTransforms.setStorageQueueSize(10000)
      ),
      loggerFactory,
    ),
    toxiproxyPlugin,
    external,
  ).foreach(registerPlugin)

  // TODO(#16089): Re-enable this test
  "introduce database connection faults on the sequencers while the runners are running" ignore {
    implicit env =>
      import env.*

      val sequencers = Seq(
        remoteSequencer1,
        remoteSequencer2,
        remoteSequencer3,
      )

      val sequencerProxies = Seq(
        replica1Proxy,
        replica2Proxy,
        replica3Proxy,
      )

      runWithFault(participant1.name, participant1.config.ledgerApi.port) { hasCompleted =>
        def failDbConnection(done: Boolean): Unit = {
          // Find the sequencer to which the participant is connected
          val (sequencer, sequencerIndex) = sequencers.zipWithIndex.find { case (seq, _) =>
            seq.health.status.trySuccess.connectedParticipants
              .contains(participant1.id)
          }.value

          val proxy = sequencerProxies(sequencerIndex)

          def withSeqHealthStubs[T](f: (HealthGrpc.HealthStub, HealthGrpc.HealthStub) => T) = {
            val remoteSequencerHealth = env.actualConfig
              .remoteSequencersByString(sequencer.name)
              .grpcHealth
              .value
            val remoteSequencerPublicApi = env.actualConfig
              .remoteSequencersByString(sequencer.name)
              .publicApi
            withHealthStubPairs(
              Seq(
                remoteSequencerHealth.address -> remoteSequencerHealth.port,
                remoteSequencerPublicApi.address -> remoteSequencerPublicApi.port,
              )
            ) { case Seq(health, seq) =>
              f(health, seq)
            }
          }

          def checkSeqServing = withSeqHealthStubs { case (health, seq) =>
            checkServing(health)
            checkServing(seq)
            checkServing(seq, CantonGrpcUtil.sequencerHealthCheckServiceName)
          }

          def checkSeqNotServing = withSeqHealthStubs { case (health, seq) =>
            checkNotServing(health)
            checkNotServing(seq)
            checkNotServing(seq, CantonGrpcUtil.sequencerHealthCheckServiceName)
          }

          if (!done) {

            checkSeqServing

            // Slow down and terminate the connection
            val toxic = proxy.underlying
              .toxics()
              .timeout("db-con-timeout", ToxicDirection.UPSTREAM, 1000)

            eventually() {
              sequencer.health.active shouldBe false
            }

            eventually() {
              // Make sure the participant was indeed disconnected from the sequencer
              sequencer.health.status.trySuccess.connectedParticipants
                .contains(participant1.id) shouldBe false

              // There should be another sequencer which now has a subscription for participant1
              sequencers.zipWithIndex.exists { case (seq, i) =>
                i != sequencerIndex && seq.health.status.trySuccess.connectedParticipants
                  .contains(participant1.id)
              } shouldBe true
              checkSeqNotServing
            }

            toxic.remove()

            eventually() {
              sequencer.health.active shouldBe true
              checkSeqServing
            }
          }
        }

        (1 to 10).foreach(_ => failDbConnection(hasCompleted()))
      }

  }

  override protected def matchAcceptableErrorOrWarnMessage: Matcher[String] = (
    super.matchAcceptableErrorOrWarnMessage.or {
      include("RequestRefused(Unavailable(Unavailable))") or
        include("Request failed for sequencer") or
        include("LOCAL_VERDICT_TIMEOUT") or
        include("Sequencing result message timed out.") or
        include("Submission timed out") or
        include("Failed to close sequencer subscription")
    }
  )

}
