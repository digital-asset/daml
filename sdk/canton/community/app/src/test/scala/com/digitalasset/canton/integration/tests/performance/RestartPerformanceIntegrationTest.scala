// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.performance.*
import com.digitalasset.canton.performance.elements.DriverStatus.MasterStatus

import scala.concurrent.Future
import scala.concurrent.duration.*

abstract class RestartPerformanceIntegrationTest extends BasePerformanceIntegrationTest {
  import BasePerformanceIntegrationTest.*

  "restart performance test" in { implicit env =>
    import env.*

    logger.debug("Starting restart performance test")

    val (p1Config, p2Config) = defaultConfigs(
      index = 2,
      participant1,
      participant2,
      reportFrequency = 10,
      totalCycles = 200,
      rateSettings = RateSettings.defaults,
    )
    val runnerP1 =
      new PerformanceRunner(
        p1Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant1-fst"),
      )
    val runnerP2 =
      new PerformanceRunner(
        p2Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant2-fst"),
      )
    env.environment.addUserCloseable(runnerP1)
    env.environment.addUserCloseable(runnerP2)
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        runnerP1
          .startup()
          .discard[Future[Either[String, Unit]]] // Errors are logged as part of startup
        runnerP2
          .startup()
          .discard[Future[Either[String, Unit]]] // Errors are logged as part of startup

        def progress(): Long =
          runnerP1
            .status()
            .collectFirst { case MasterStatus(_, _, _, _, totalProposals, totalApprovals) =>
              totalProposals + totalApprovals
            }
            .getOrElse(0)

        eventually(60.seconds) {
          progress() should be > 3L
        }

        runnerP1.setActive(false)
        runnerP2.setActive(false)
        // let system quiet down so we have less commands in-flight when restarting and create less noise
        Threading.sleep(2000)
        runnerP1.close()
        runnerP2.close()
      },
      forEvery(_)(acceptableLogMessage),
    )

    logger.debug("Resuming operations")

    val runnerP1a =
      new PerformanceRunner(
        whackOnStartup(p1Config),
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant1-snd"),
      )
    val runnerP2a =
      new PerformanceRunner(
        whackOnStartup(p2Config),
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant2-snd"),
      )
    env.environment.addUserCloseable(runnerP1a)
    env.environment.addUserCloseable(runnerP2a)

    // suppress warnings and errors as we might have conflicts with inflight commands after startup
    loggerFactory.suppressWarningsAndErrors {
      val f1 = runnerP1a.startup()
      val f2 = runnerP2a.startup()
      waitUntilComplete((runnerP1a, f1), (runnerP2a, f2))
    }
  }
}

// not testing H2 due to known problems

class RestartPerformanceIntegrationTestPostgres extends RestartPerformanceIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))
}
