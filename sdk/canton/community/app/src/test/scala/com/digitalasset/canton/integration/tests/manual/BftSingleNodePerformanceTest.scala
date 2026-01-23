// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.testing.MetricValues
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalMediatorReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest.defaultConfigs
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.performance.{PerformanceRunner, RateSettings}
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.Instant

/** Integration test for single-node DA BFT deployment that reports TPS and latency.
  *
  * This test was implemented based on
  * [[com.digitalasset.canton.integration.tests.nightly.sequencer.SequencerCatchUpPerformanceIntegrationTest]]
  *
  * Usage:
  *
  * Run in the sbt console:
  * {{{
  * sbt "community-app/testOnly *.BftSingleNodePerformanceTest"
  * }}}
  *
  * This test measures the throughput (TPS) and latency of a single DA BFT sequencer node with 2
  * participants and 1 mediator. It reports baseline performance numbers without any optimization
  * for a trusted, centralized setup.
  *
  * Check the results in the test output or in the log folder:
  *
  * {{{
  * cat canton_test.log | grep 'DA BFT Single Node Performance Results'
  * }}}
  *
  * Configuration:
  *   - `loadTestDurationMillis`: Duration to run the load test (default: 5 minutes)
  *   - `enableTrafficManagement`: Enable/disable traffic management (default: true)
  */
class BftSingleNodePerformanceTest
    extends BasePerformanceIntegrationTest
    with SharedEnvironment
    with MetricValues
    with TrafficBalanceSupport {

  // Manipulate the settings to try different options and scenarios

  // Duration to run the load test (in milliseconds)
  private val loadTestDurationMillis = 5 * 60000L

  // Enable traffic management
  private val enableTrafficManagement = true

  registerPlugin(
    new UsePostgres(
      loggerFactory,
      customMaxConnectionsByNode = Some(_ => Some(PositiveInt.tryCreate(10))),
    )
  )
  registerPlugin(new UseBftSequencer(loggerFactory))

  private def allMediators()(implicit env: TestConsoleEnvironment): Seq[LocalMediatorReference] = {
    import env.*
    Seq(mediator1)
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 1,
        numMediators = 1,
      )
      .withManualStart
      .withSetup { implicit env =>
        import env.*

        allMediators().foreach(_.start())

        sequencer1.start()

        participant1.start()
        participant2.start()

        allMediators().foreach(m =>
          clue(s"${m.name} wait for ready for init") {
            m.health.wait_for_ready_for_initialization()
          }
        )

        sequencer1.health.wait_for_ready_for_initialization()
      }
      .withNetworkBootstrap { implicit env =>
        import env.*

        val allMediators_ = allMediators()
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = allMediators_,
            overrideMediatorToSequencers = Some(
              allMediators_.map { mediator =>
                mediator -> (Seq(sequencer1), PositiveInt.one, NonNegativeInt.zero)
              }.toMap
            ),
            mediatorThreshold = PositiveInt.one,
          )
        )
      }
      .addConfigTransforms { config =>
        config
          .focus(_.parameters.enableAdditionalConsistencyChecks)
          .replace(false)
          .focus(_.participants)
          .each
          .modify(
            _.focus(_.parameters.ledgerApiServer.indexer.ingestionParallelism)
              .replace(NonNegativeNumeric.tryCreate(2))
          )
      }
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.enableAdditionalConsistencyChecks)
            .replace(false)
        )
      )

  private val testLogger = loggerFactory.getLogger(getClass)

  private val trafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(1L),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(10L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.tryCreate(1),
  )

  def blockEventCount(sequencer: LocalSequencerReference): Long =
    sequencer.underlying.value.sequencer.metrics.block.blockEvents.valuesWithContext.toList
      .map(_._2)
      .sum

  "Test DA BFT in single-node mode (collect TPS and latency)" in { implicit env =>
    import env.*

    val (p1Config, p2Config) =
      defaultConfigs(
        0,
        participant1,
        participant2,
        totalCycles =
          1000000000, // Any big number, we control the duration with loadTestDurationMillis
        rateSettings = RateSettings.defaults,
      )

    val runnerP1 =
      new PerformanceRunner(
        p1Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant1"),
      )
    val runnerP2 =
      new PerformanceRunner(
        p2Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant2"),
      )

    val runners = Seq(runnerP1, runnerP2)

    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connects to sequencer1") {
      participant2.synchronizers.connect_local(sequencer1, daName)
    }

    if (enableTrafficManagement) {
      initializedSynchronizers.foreach { case (_, synchronizer) =>
        synchronizer.synchronizerOwners.foreach { owner =>
          owner.topology.synchronizer_parameters.propose_update(
            synchronizerId = synchronizer.synchronizerId,
            _.update(
              trafficControl = Some(trafficControlParameters)
            ),
          )
        }
      }

      val balance = PositiveLong.tryCreate(Long.MaxValue)

      updateBalanceForMember(participant1, balance)
      updateBalanceForMember(participant2, balance)
      allMediators().foreach(
        updateBalanceForMember(_, balance)
      )

    }

    // ping both participants
    participant2.health.ping(participant1.id)
    participant1.health.ping(participant2.id)

    runners.foreach(env.environment.addUserCloseable(_))

    testLogger.info("Starting load test")
    val startTime = Instant.now()
    val eventCountBefore = blockEventCount(sequencer1)

    // Start load generation
    runners.foreach(_.startup().discard)

    // Run for the configured duration
    Threading.sleep(loadTestDurationMillis)

    // Stop load generation
    runners.foreach(_.close())

    val endTime = Instant.now()
    val eventCountAfter = blockEventCount(sequencer1)

    // Calculate TPS
    def instantToSeconds(instant: Instant): Double =
      instant.getEpochSecond.toDouble + (instant.getNano.toDouble / 1000_000_000)

    val durationSeconds = instantToSeconds(endTime) - instantToSeconds(startTime)
    val totalEvents = eventCountAfter - eventCountBefore
    val tps = totalEvents.toDouble / durationSeconds

    // Get latency metrics from sequencer
    val sequencerMetrics = sequencer1.underlying.value.sequencer.metrics
    val blockDelayMs = sequencerMetrics.block.delay.getValue

    val latencyStats = s"block delay: ${blockDelayMs}ms"

    val resultsMessage = s"""
      |=== DA BFT Single Node Performance Results ===
      |Configuration:
      |  - Participants: 2
      |  - Sequencers: 1 (DA BFT)
      |  - Mediators: 1
      |  - Duration: ${durationSeconds.toInt}s
      |
      |Results:
      |  - Total Events: $totalEvents
      |  - TPS (Transactions Per Second): ${tps.toInt}
      |  - Latency: $latencyStats
      |=============================================""".stripMargin

    println(resultsMessage)
  }

  private def updateBalanceForMember(
      instance: LocalInstanceReference,
      newBalance: PositiveLong,
  )(implicit env: TestConsoleEnvironment): Assertion =
    updateBalanceForMember(
      instance,
      newBalance,
      () => (),
    )
}
