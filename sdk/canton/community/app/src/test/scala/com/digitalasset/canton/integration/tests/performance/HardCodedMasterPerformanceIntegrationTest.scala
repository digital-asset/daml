// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.performance.*
import monocle.macros.syntax.lens.*

abstract class HardCodedMasterPerformanceIntegrationTest extends BasePerformanceIntegrationTest {
  import BasePerformanceIntegrationTest.*

  "properly startup if master name is hard-coded and we have 1000 assets per issuer" in {
    implicit env =>
      import env.*

      // issue 1000 assets, because this has caused trouble from time to time
      val (p1Config, p2Config) = defaultConfigs(
        1,
        participant1,
        participant2,
        numAssetsPerIssuer = 1000,
        rateSettings = RateSettings.defaults,
      )
      val runnerP1 =
        new PerformanceRunner(
          p1Config,
          _ => NoOpMetricsFactory,
          loggerFactory.append("participant", "participant1"),
        )
      env.environment.addUserCloseable(runnerP1)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          // start first runner
          val f1 = runnerP1.startup()
          // wait until master is ready and known to participant2
          eventually() {
            participant2.parties.list(filterParty = p1Config.master) should not be empty
          }
          // fire up other performance runner that just uses the party name directly
          val master = participant2.parties
            .list(filterParty = p1Config.master)
            .headOption
            .getOrElse(throw new IllegalArgumentException("master was there and it's gone ..."))
            .party

          val runnerP2 =
            new PerformanceRunner(
              p2Config
                .focus(_.master)
                .replace(master.toProtoPrimitive),
              _ => NoOpMetricsFactory,
              loggerFactory.append("participant", "participant2"),
            )
          env.environment.addUserCloseable(runnerP2)
          val f2 = runnerP2.startup()
          waitUntilComplete((runnerP1, f1), (runnerP2, f2))
        },
        forEvery(_)(acceptableLogMessage),
      )
  }
}

// not testing H2 due to known problems

class HardCodedMasterPerformanceIntegrationTestPostgres
    extends HardCodedMasterPerformanceIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))

}
