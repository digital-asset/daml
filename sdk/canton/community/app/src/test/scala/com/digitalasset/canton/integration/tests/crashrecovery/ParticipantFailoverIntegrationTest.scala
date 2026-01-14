// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.integration.plugins.UsePostgres
import monocle.macros.syntax.lens.*

trait ParticipantFailoverIntegrationTest extends ParticipantFailoverIntegrationTestBase {

  "A replicated participant" must {
    val reliabilityTest =
      ReliabilityTest(
        Component(
          name = "application",
          setting =
            "application connected to replicated participant which queries node status to find active participant replica",
        ),
        AdverseScenario(
          dependency = "participant node",
          details = "forceful stop (and then restart) of the active participant's process",
        ),
        Remediation(
          remediator = "application",
          action =
            "fail-over to a new active participant replica while stopped participant restarts",
        ),
        outcome = "transaction processing continuously possible given proper retries",
      )

    "fail-over when the active replica crashes multiple times".taggedAs(
      reliabilityTest
    ) in { implicit env =>
      import env.*

      val p1 = rp("participant1")
      val p2 = rp("participant2")

      for (i <- 1 to 3) {
        logger.debug(s"Crashing active replica (#$i)..")
        restartActiveReplica(p1, p2, participant3)
      }

    }

    // TODO(test-coverage): Test crash of participant node in a long-running test, not just 3 crashes
    // TODO(test-coverage): Test with a reverse proxy such that fail-over is transparent to the application

    "indicate the replica's activeness through the health check".taggedAs(
      reliabilityTest
        .focus(_.component.setting)
        .replace("application queries health endpoint to find active participant replica")
    ) in { implicit env =>
      import env.*

      val p1 = rp("participant1")
      val p2 = rp("participant2")

      val (activeParticipant, passiveParticipant) = if (isActive(p1)) (p1, p2) else (p2, p1)

      eventually(timeout) {
        assert(isHealthy(activeParticipant.name))
        assert(!isHealthy(passiveParticipant.name))
      }

      // Kill the active participant instance, then the passive one must become healthy
      externalPlugin.kill(activeParticipant.name)
      eventually(timeout) {
        assert(isHealthy(passiveParticipant.name))
      }
    }
  }

}

class ParticipantFailoverIntegrationTestPostgres extends ParticipantFailoverIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))
}
