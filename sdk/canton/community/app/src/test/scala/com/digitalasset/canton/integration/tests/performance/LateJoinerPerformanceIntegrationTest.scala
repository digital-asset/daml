// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.performance.*
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings

trait LateJoinerPerformanceIntegrationTest extends BasePerformanceIntegrationTest {
  import BasePerformanceIntegrationTest.*

  "run a normal performance test with late joiners" in { implicit env =>
    import env.*

    val (p1Config, p2Config) = defaultConfigs(
      0,
      participant1,
      participant2,
      withPartyGrowth = 1,
      // adding full contention
      rateSettings =
        RateSettings(SubmissionRateSettings.TargetLatency(duplicateSubmissionRatio = 1.0)),
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
        val f2 = runnerP2.startup()
        val f1 = runnerP1.startup()
        waitUntilComplete((runnerP1, f1), (runnerP2, f2))
      },
      forEvery(_)(acceptableLogMessage),
    )
  }
}

// not testing H2 due to known problems

class LateJoinerPerformanceIntegrationTestPostgres extends LateJoinerPerformanceIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))
}
