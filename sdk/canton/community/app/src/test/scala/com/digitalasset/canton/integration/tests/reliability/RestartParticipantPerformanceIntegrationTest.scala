// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.integration.ConfigTransforms
import com.digitalasset.canton.integration.plugins.{UseConfigTransforms, UsePostgres}
import com.digitalasset.canton.integration.tests.ReplicatedNodeHelper
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.logging.LogEntry
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.Assertion

import scala.concurrent.duration.DurationInt
import scala.util.Try

class RestartParticipantPerformanceIntegrationTest
    extends ReliabilityPerformanceIntegrationTest
    with BasePerformanceIntegrationTest
    with ReplicatedNodeHelper {

  "restart the participants while the runners are running".taggedAs(
    ReliabilityTest(
      Component(name = "Ping application", setting = "connected to non-replicated participant"),
      dependencyFailure = AdverseScenario(
        dependency = "Participant",
        details = "Orderly restart (`.stop()` and `.start()`)",
      ),
      Remediation(
        remediator = "Application",
        action = "Application should retry commands until successful",
      ),
      outcome =
        "transaction processing possible whenever the participant is not currently restarted",
    )
  ) in { implicit env =>
    import env.*

    runWithFault(
      participant1.name,
      participant1.config.ledgerApi.port,
      withPartyGrowth = 1, // use party growth to add topology mgmt crash
    ) { hasCompleted =>
      def restart(done: Boolean): Unit =
        if (!done) {
          clue("stopping participant") {
            // As the participant is loaded, this can fail with an exception.
            // Catching the exception so that the test does not fail.
            Try(participant1.stop())
          }
          clue("starting participant") {
            participant1.start()
          }
          clue("waiting for participant to be running again") {
            participant1.health.wait_for_running()
          }
          clue("waiting for participant to be active") {
            waitActive(participant1)
          }
          clue("reconnect to synchronizers") {
            // start without synchronisation of topology state, as the performance runner will keep on adding new
            // parties, so we'll never get a quiet topology state
            // TODO(#9367) smarter topology synchronisation
            participant1.synchronizers.reconnect_all(synchronize = None)
          }
          // sleeping to give the system time to catch up (else we often run into timeout issues due to, e.g.,
          // still being busy with the replay/crashrecovery of previous messages when the new shutdown already starts)
          Threading.sleep(120000)
        }

      // restart this beast 5 times
      (1 to 5).foreach(_ => restart(hasCompleted()))
    }

  }

  // Whitelisting all errors and warnings, as the list of problems that can be reported during shutdown is too long.
  override protected def acceptableErrorOrWarning(entry: LogEntry): Assertion = succeed

  setupPlugins(new UsePostgres(loggerFactory))

  registerPlugin(
    new UseConfigTransforms(
      Seq(
        ConfigTransforms.setStorageQueueSize(10000),
        _.focus(_.parameters.timeouts.processing.shutdownShort)
          .replace(NonNegativeDuration.tryFromDuration(15.seconds)), // from 3 seconds
      ),
      loggerFactory,
    )
  )
}
