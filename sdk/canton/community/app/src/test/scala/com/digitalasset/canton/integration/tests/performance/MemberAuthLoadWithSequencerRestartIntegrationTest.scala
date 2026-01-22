// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.elements.DriverStatus
import com.digitalasset.canton.performance.{PerformanceRunner, RateSettings}
import monocle.macros.syntax.lens.*

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** Run a performance test with lots of auth token expirations
  *
  * This test runs the performance runner together with very aggressive auth token limitations. This
  * means that the client connections will be restarted very frequently.
  *
  * In addition, the test will stop and start sequencers repeatedly, alternating between sequencer1
  * and sequencer2.
  */
abstract class MemberAuthLoadWithSequencerRestartIntegrationTest
    extends BasePerformanceIntegrationTest {
  import BasePerformanceIntegrationTest.*

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S2M1
      .addConfigTransforms(
        _.focus(_.parameters.timeouts.processing.shutdownProcessing)
          .replace(NonNegativeDuration.tryFromDuration(1.minute)),
        // Consistency checks are slow, so we should not use them in performance tests
        _.focus(_.parameters.enableAdditionalConsistencyChecks).replace(false),
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.publicApi.maxTokenExpirationInterval)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(2))
        ),
        ConfigTransforms.updateAllSequencerClientConfigs_(
          _.focus(_.authToken.refreshAuthTokenBeforeExpiry)
            .replace(config.NonNegativeFiniteDuration.ofMillis(500))
            .focus(_.initialConnectionRetryDelay)
            .replace(config.NonNegativeFiniteDuration.ofMillis(0))
            .focus(_.maxConnectionRetryDelay)
            .replace(config.NonNegativeFiniteDuration.ofMillis(250))
            .focus(_.useNewConnectionPool) // works for both
            .replace(true)
            .focus(_.enableAmplificationImprovements) // should be true anyway
            .replace(true)
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.foreach(
          _.synchronizers.connect_local_bft(
            sequencers.all,
            "synchronizer",
            sequencerTrustThreshold = 1,
          )
        )

        performanceTestSetup
      }

  "run a performance test with lots of auth reconnects for a minute" in { implicit env =>
    import env.*

    val (p1Config, p2Config) = defaultConfigs(
      0,
      participant1,
      participant2,
      withPartyGrowth = 1,
      totalCycles = 1000,
      rateSettings =
        RateSettings(submissionRateSettings = SubmissionRateSettings.FixedRate(1.0), batchSize = 1),
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

    env.environment.addUserCloseable(runnerP1)
    env.environment.addUserCloseable(runnerP2)

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        runnerP1.startup().discard
        runnerP2.startup().discard

        // wait until everything is started
        logger.info(s"Waiting for perf runner to be active")
        utils.retry_until_true {
          val stats = Seq(runnerP1, runnerP2).flatMap(_.status())
          stats.sizeIs >= 4 && stats.forall {
            case c: DriverStatus.TraderStatus =>
              c.mode == "THROUGHPUT"
            case _: DriverStatus.MasterStatus => true
          }
        }

        (1 to 4).foreach { idx =>
          val sequencer = if (idx % 2 == 0) sequencer1 else sequencer2
          logger.info(s"Iteration $idx: stopping " + sequencer.name)
          sequencer.stop()
          Threading.sleep(1000)
          logger.info(s"Iteration $idx: starting " + sequencer.name)
          sequencer.start()
          Threading.sleep(4000)
        }
        val cf = Future.sequence(Seq(runnerP1, runnerP2).map(_.closeF()))
        cf.futureValue
      },
      forEvery(_)(
        acceptableLogMessageExt(
          Seq(
            // some requests may fail submission
            "Is the server running? Did you configure the server",
            // some topology transactions must be retried
            "failed the following topology transaction",
          ),
          Seq(),
        )
      ),
    )
  }

}

class MemberAuthLoadWithSequencerRestartIntegrationTestPostgres
    extends MemberAuthLoadWithSequencerRestartIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))
}
