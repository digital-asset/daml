// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.admin.api.client.data.TemplateId.templateIdsFromJava
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.performance.*
import com.digitalasset.canton.performance.PartyRole.{
  DvpIssuer,
  DvpTrader,
  Master,
  MasterDynamicConfig,
}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.elements.AmendMasterConfig.AmendMasterConfigDvp
import com.digitalasset.canton.performance.elements.DriverStatus.MasterStatus
import com.digitalasset.canton.performance.elements.dvp.TraderDriver
import com.digitalasset.canton.performance.model.java.orchestration.{Role, TestRun, runtype}

import scala.concurrent.duration.*

sealed trait ChangeScenarioPerformanceIntegrationTest extends BasePerformanceIntegrationTest {
  import BasePerformanceIntegrationTest.*

  "modify the run type after a while" in { implicit env =>
    import env.*

    val rate = RateSettings(
      SubmissionRateSettings.TargetLatency(startRate = 0.5, targetLatencyMs = 2000),
      batchSize = 1,
    )
    val config = PerformanceRunnerConfig(
      master = s"Master",
      localRoles = Set(
        Master(
          s"Master",
          runConfig = MasterDynamicConfig(
            totalCycles = 40,
            reportFrequency = 1,
            runType = new runtype.DvpRun(
              20L,
              0,
              0,
              TraderDriver.toPartyGrowth(1),
            ),
          ),
          amendments = Seq(new AmendMasterConfigDvp("test run") {
            override def checkDvp(stats: MasterStatus, master: TestRun, dvp: runtype.DvpRun)
                : Option[MasterDynamicConfig] =
              Option.when(master.mode == model.java.orchestration.Mode.THROUGHPUT)(
                // adjust to acsGrowth when we started
                MasterDynamicConfig
                  .fromContract(master)
                  .copy(
                    reportFrequency = 10,
                    totalCycles = 50,
                    runType = new runtype.DvpRun(
                      dvp.numAssetsPerIssuer,
                      1,
                      dvp.payloadSize,
                      dvp.partyGrowth,
                    ),
                  )
              )
          }),
        ),
        DvpTrader(s"Tradie1", settings = rate),
        DvpTrader(s"Tradie2", settings = rate),
        DvpTrader(s"Tradie3", settings = rate),
        DvpTrader(s"Tradie4", settings = rate),
        DvpIssuer(s"Issuator1", settings = RateSettings.defaults),
        DvpIssuer(s"Issuator2", settings = RateSettings.defaults),
      ),
      ledger = toConnectivity(participant1),
      baseSynchronizerId = daId,
    )

    val runnerP1 =
      new PerformanceRunner(
        config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant1"),
      )

    env.environment.addUserCloseable(runnerP1)

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      waitTimeout.await("runnerP1.startup()")(runnerP1.startup()),
      forEvery(_)(acceptableLogMessage),
    )

    val masterParty =
      participant1.parties.list(filterParty = "Master").map(_.party).headOption.value

    // check that all traders report 50 proposals (we can't check for accepts as they might be asymmetrically distributed
    // because of that they won't get reported.
    eventually(timeUntilSuccess = 60.seconds) {
      val results = participant1.ledger_api.javaapi.state.acs
        .filter(model.java.orchestration.TestParticipant.COMPANION)(
          masterParty
        )
      forAll(results.filter(_.data.role match {
        case Role.ISSUER => false
        case Role.TRADER => true
      }))(_.data.proposed shouldBe 50)
    }

    // in the end, we should see a couple of ACS growth contracts
    participant1.ledger_api.state.acs.of_all(filterTemplates =
      templateIdsFromJava(model.java.dvp.asset.AcsGrowth.TEMPLATE_ID)
    ) should not be empty

  }
}

class ChangeScenarioPerformanceIntegrationTestPostgres
    extends ChangeScenarioPerformanceIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))
}
