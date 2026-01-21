// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.sequencer

import cats.syntax.option.*
import com.daml.metrics.HistogramDefinition
import com.daml.metrics.api.MetricQualification
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.testing.MetricValues
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
  PositiveLong,
}
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
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{SequencerToPostgres, UseToxiproxy}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseExternalProcess,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest.defaultConfigs
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, NodeLoggingUtil}
import com.digitalasset.canton.metrics.MetricsConfig.JvmMetrics
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.performance.{PerformanceRunner, RateSettings}
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.synchronizer.sequencer.{
  BlockSequencerStreamInstrumentationConfig,
  SequencerConfig,
}
import eu.rekawek.toxiproxy.model.ToxicDirection
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.Instant
import scala.concurrent.duration.*

/** Usage:
  *
  * Run in the sbt console:
  * {{{
  * community-app/testOnly *.SequencerCatchUpPerformanceIntegrationTest
  * }}}
  *
  * Check the events/s in the `log` folder after the run:
  *
  * {{{
  * cat canton_test.log | grep 'sequencer2 events/s'
  * }}}
  *
  * More options:
  *
  * Try different options in `SequencerCatchUpPerformanceIntegrationTest` to produce different
  * scenarios.
  *
  * Go through the comments in this PR and comment out/uncomment certain parts to check differences.
  *
  * Working with Prometheus & Grafana:
  *
  *   - Set up the postgres locally, so the test doesn't create the test container each time.
  *     Example envs:
  *
  * {{{
  * export CI=1
  * export POSTGRES_USER=postgres
  * export POSTGRES_PASSWORD=''
  * export POSTGRES_DB=postgres
  * }}}
  *
  *   - Open the `community/app/src/test/resources/examples/13-observability` folder.
  *   - Adjust the `docker-compose-observability.yml` file to reflect your postgres setup.
  *   - Uncomment the integration_tests job in `prometheus/prometheus.yml`
  *   - Run `docker compose -f docker-compose-observability.yml up` (or with `-d` option if you
  *     prefer)
  *   - Change the following settings in the test:
  *
  * {{{
  * private val exposeHttpMetrics = true
  * }}}
  *
  * {{{
  * private val useExternalSequencerProcess = true
  * }}}
  *
  * If there's a problem with the external process, run `sbt dumpClassPath` and try again.
  *
  * You may also want to increase `beforeRestartDurationMillis` so you have a more time to gather
  * the metrics.
  *
  *   - Prometheus: http://localhost:9090/
  *   - Grafana: http://localhost:3000/
  *
  * To check events/s, visit the "canton-network > Sequencer Traffic" diagram.
  *
  * Attaching a profiler:
  *
  *   - Run the tests with `useExternalSequencerProcess` enabled.
  *   - Wait for the sequencer to restart (you can see a new java process)
  *   - Attach a profiler to the new java process. Make sure you stop profiling before the process
  *     stops.
  */
class SequencerCatchUpPerformanceIntegrationTest
    extends BasePerformanceIntegrationTest
    with SharedEnvironment
    with MetricValues
    with TrafficBalanceSupport {

  // Manipulate the settings to try different options and scenarios

  // Enable to see metrics in Prometheus or Grafana. For some reason, doesn't work with the 'countBlockEvents' flag
  private val exposeHttpMetrics = false

  // Enable to automatically count events/s, grep 'sequencer2 events/s' after the test run
  // Set to true by default, so we can print the sequencer speed on CircleCI
  private val countBlockEvents = true

  // Enable to run the sequencer in a separate JVM process. Useful if you want to profile the sequencer in isolation
  private val useExternalSequencerProcess = false

  // Enable toxi proxy to simulate the database latency
  private val useToxiProxy = true

  // The duration of load producing before the restart. The bigger the number, the more events will accumulate to catch up with
  private val beforeRestartDurationMillis = 5 * 60000L

  // The max time for the sequencer2 to catch up
  private val afterRestartDurationMillis = beforeRestartDurationMillis / 2

  // Useful to isolate the sequencer2 performance
  private val stopOtherNodesDuringCatchUp = true

  // Keep producing load after catch-up. May result in sequencer2 never catching up, depending on the other settings.
  private val produceLoadAfterRestart = false

  private val enableTrafficManagement = true

  // use this flag to turn off debug logging during the catch-up
  private val turnOffDebugLogging = false

  private val metricsConfigTransform: ConfigTransform = config =>
    config
      .focus(_.monitoring.metrics)
      .replace(
        MetricsConfig(
          reporters = Seq(
            MetricsReporterConfig.Prometheus(
              address = "0.0.0.0"
            )
          ),
          jvmMetrics = Some(JvmMetrics(enabled = true)),
          qualifiers = MetricQualification.All,
          histograms = Seq(
            HistogramDefinition(
              name = "*",
              aggregation = HistogramDefinition.Exponential(
                maxBuckets = 160,
                maxScale = 20,
              ),
            )
          ),
        )
      )

  private val streamInstrumentationConfigTransform: ConfigTransform = config =>
    config
      .focus(_.sequencers)
      .modify { sequencers =>
        sequencers.map { case (k, sequencer) =>
          val newSequencer = sequencer.copy(
            sequencer = sequencer.sequencer match {
              case ext: SequencerConfig.External =>
                ext.copy(
                  block = ext.block.copy(
                    streamInstrumentation =
                      BlockSequencerStreamInstrumentationConfig(isEnabled = true)
                  )
                )
              case _ =>
                ???
            }
          )

          (k, newSequencer)
        }
      }

  lazy val externalProcessOpt: Option[UseExternalProcess] =
    Option.when(useExternalSequencerProcess)(
      new UseExternalProcess(
        loggerFactory,
        externalSequencers = Set("sequencer2"),
        fileNameHint = this.getClass.getSimpleName,
        configTransforms =
          if (exposeHttpMetrics) Seq(metricsConfigTransform, streamInstrumentationConfigTransform)
          else Seq(streamInstrumentationConfigTransform),
      )
    )

  private val sequencer2Proxy = "sequencer2-to-postgres"

  private val toxiproxyPluginOpt: Option[UseToxiproxy] = Option.when(useToxiProxy)(
    new UseToxiproxy(
      ToxiproxyConfig(proxies = Seq(SequencerToPostgres(sequencer2Proxy, "sequencer2")))
    )
  )

  private lazy val isCi: Boolean = sys.env.isDefinedAt("CI")

  registerPlugin(
    new UsePostgres(
      loggerFactory,
      customMaxConnectionsByNode = Some {
        case "sequencer2" => PositiveInt.tryCreate(10).some
        case _ => PositiveInt.tryCreate(5).some
      },
    )
  )
  toxiproxyPluginOpt.foreach(registerPlugin)
  registerPlugin(new UseBftSequencer(loggerFactory))

  externalProcessOpt.foreach(registerPlugin)

  private def allMediators()(implicit env: TestConsoleEnvironment): Seq[LocalMediatorReference] = {
    import env.*
    Seq(mediator1, mediator2, mediator3, mediator4, lm("mediator5"))
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 2,
        numMediators = 5,
      )
      .withManualStart
      .withTeardown { _ =>
        toxiproxyPluginOpt.foreach { toxiproxyPlugin =>
          ToxiproxyHelpers.removeAllProxies(
            toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
          )
        }
      }
      .withSetup { implicit env =>
        import env.*

        allMediators().foreach(_.start())

        sequencer1.start()
        startSequencer()

        participant1.start()
        participant2.start()

        allMediators().foreach(m =>
          clue(s"${m.name} wait for ready for init") {
            m.health.wait_for_ready_for_initialization()
          }
        )

        sequencer1.health.wait_for_ready_for_initialization()

        val seq2 = if (useExternalSequencerProcess) remoteSequencer2 else sequencer2
        seq2.health.wait_for_ready_for_initialization()
      }
      .withNetworkBootstrap { implicit env =>
        import env.*

        val seq2 = if (useExternalSequencerProcess) remoteSequencer2 else sequencer2
        val allMediators_ = allMediators()
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1, seq2),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1, seq2),
            mediators = allMediators_,
            overrideMediatorToSequencers = Some(
              allMediators_.map { mediator =>
                // Make sure both mediators are connected the first sequencer to avoid the SEQUENCER_SUBSCRIPTION_LOST warning
                // And flaky results (load produced super slowly from time to time)
                mediator -> (Seq(sequencer1), PositiveInt.one, NonNegativeInt.zero)
              }.toMap
            ),
            mediatorThreshold = allMediators_.length - 1,
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
      .addConfigTransform(streamInstrumentationConfigTransform)
      .addConfigTransform(
        if (exposeHttpMetrics && !useExternalSequencerProcess) metricsConfigTransform
        else identity
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

  "Test the speed of the sequencer catch-up" in { implicit env =>
    import env.*

    val (p1Config, p2Config) =
      defaultConfigs(
        0,
        participant1,
        participant2,
        rateSettings = RateSettings.defaults,
        // Any big number, we control the duration with other settings (private vals of this class)
        totalCycles = 1000000000,
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

    testLogger.info("sequencer2 STOPPING")
    stopSequencer()

    runners.foreach(env.environment.addUserCloseable(_))

    // Time of the test is controlled in the private vals of the class
    runners.foreach(_.startup().discard)

    Threading.sleep(beforeRestartDurationMillis)

    if (!produceLoadAfterRestart) {
      runners.foreach(_.close())
    }

    if (stopOtherNodesDuringCatchUp) {
      participant1.stop()
      participant2.stop()
      allMediators().foreach(_.stop())
    }

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        testLogger.info("sequencer2 RESTARTING")

        // enable db toxiproxy
        toxiproxyPluginOpt.foreach { toxiproxyPlugin =>
          val proxy = toxiproxyPlugin.runningToxiproxy.getProxy(sequencer2Proxy)
          val client = proxy
            .valueOrFail("must be here")
            .underlying
          client.toxics().latency("sequencer-db", ToxicDirection.UPSTREAM, 3)
        }

        if (turnOffDebugLogging)
          NodeLoggingUtil.setLevel(level = "INFO")
        startSequencer()

        val before = Instant.now()
        val seq2EventCountBeforeOpt: Option[Long] =
          if (exposeHttpMetrics || !countBlockEvents) None else Some(blockEventCount(sequencer2))

        def instantToSeconds(instant: Instant): Double =
          instant.getEpochSecond.toDouble + (instant.getNano.toDouble / 1000_000_000)

        seq2EventCountBeforeOpt match {
          case Some(seq2EventCountBefore) =>
            eventually(
              timeUntilSuccess = afterRestartDurationMillis milliseconds,
              maxPollInterval = 50 milliseconds,
            ) {
              val seq1EventCount = blockEventCount(sequencer1)
              val seq2EventCount = blockEventCount(sequencer2)

              testLogger.info(
                s"Checking if sequencer2 has caught up. sequencer1 events: $seq1EventCount, sequencer2 events: $seq2EventCount"
              )

              // seq2 has caught up if it has seen all events.
              // it may see more in case of shutdown during async processing
              val seq2CaughtUp = seq2EventCount >= seq1EventCount

              assert(seq2CaughtUp)

              if (seq2CaughtUp) {
                val now = Instant.now()
                val processedEventCount = seq2EventCount - seq2EventCountBefore
                val eventsPerSec =
                  processedEventCount.toDouble / (instantToSeconds(now) - instantToSeconds(before))

                val logMessage =
                  f"sequencer2 events/s: $eventsPerSec%.2f, processed events: $processedEventCount"
                testLogger.info(logMessage)

                // Ugly solution to print the sequencer speed on CircleCI
                // Grepping the log files won't work since the log files are gzipped after the tests
                if (isCi) println(logMessage)
              }
            }
          case None =>
            Threading.sleep(afterRestartDurationMillis)
        }
        testLogger.info("sequencer2 COMPLETED")
        if (turnOffDebugLogging)
          NodeLoggingUtil.setLevel(level = "DEBUG")
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq.empty,
        // Circuit-breaker-related warn logs are allowed
        mayContain = Seq(
          _.warningMessage should include(
            "Sequencer is unhealthy, so disconnecting all members. Overloaded. Can't receive requests at the moment"
          ),
          _.warningMessage should include(
            "Could not send a time-advancing message: RequestRefused"
          ),
          _.errorMessage should include("periodic acknowledgement failed"),
        ),
      ),
    )

    if (produceLoadAfterRestart) {
      runners.foreach(_.close())
    }
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

  private def startSequencer()(implicit env: FixtureParam): Unit = {
    import env.*

    externalProcessOpt match {
      case Some(externalProcess) =>
        externalProcess.start("sequencer2")
        assert(externalProcess.isRunning("sequencer2"))
      case None =>
        sequencer2.start()
        assert(sequencer2.is_running)
    }
  }

  private def stopSequencer()(implicit env: FixtureParam): Unit = {
    import env.*

    externalProcessOpt match {
      case Some(externalProcess) =>
        externalProcess.kill("sequencer2")
        assert(!externalProcess.isRunning("sequencer2"))
      case None =>
        sequencer2.stop()
        assert(!sequencer2.is_running)
    }
  }
}
