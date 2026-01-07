// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.integration.IntegrationTestUtilities.{
  GrabbedCounts,
  assertIncreasingRecordTime,
  grabCounts,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  IntegrationTestUtilities,
  TestConsoleEnvironment,
}

import scala.concurrent.duration.{Duration, DurationInt}

trait BongTestScenarios { this: CommunityIntegrationTest =>

  protected def setupBongTest(implicit
      env: TestConsoleEnvironment
  ): (GrabbedCounts, GrabbedCounts) = {
    import env.*
    // warm up with a ping and make sure the three warm-up ping-related events are through before counting the bong events
    assertPingSucceeds(participant1, participant2)
    eventually() {
      participant1.testing.acs_search(
        synchronizerAlias = daName,
        filterTemplate = "^Canton.Internal",
      ) shouldBe empty
      participant2.testing.acs_search(
        synchronizerAlias = daName,
        filterTemplate = "^Canton.Internal",
      ) shouldBe empty
    }

    val p1_count = grabCounts(daName, participant1, 5000)
    val p2_count = grabCounts(daName, participant2, 5000)
    (p1_count, p2_count)
  }

  protected def runBongTest(
      p1_count: GrabbedCounts,
      p2_count: GrabbedCounts,
      levels: Int,
      timeout: Duration = 30.seconds,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    val n: Int = levels

    participant1.testing.bong(
      Set(participant1.id, participant2.id),
      levels = n,
      timeout = NonNegativeDuration.tryFromDuration(timeout),
    )

    eventually() {
      val bongGrabs = IntegrationTestUtilities.expectedGrabbedCountsForBong(n)
      val p1_count2 =
        grabCounts(daName, participant1, limit = bongGrabs.plus(p1_count).maxCount + 1000)
      val p2_count2 =
        grabCounts(daName, participant2, limit = bongGrabs.plus(p2_count).maxCount + 1000)

      assertResult(bongGrabs)(p1_count2.minus(p1_count))
      // both participants have same number of events
      assertResult(bongGrabs)(p2_count2.minus(p2_count))

    }

    assertIncreasingRecordTime(participant1)
    assertIncreasingRecordTime(participant2)
  }

  protected def setupAndRunBongTest(levels: Int, timeout: Duration = 30.seconds)(implicit
      env: TestConsoleEnvironment
  ): Any = {

    val (p1_count, p2_count) = setupBongTest
    runBongTest(p1_count, p2_count, levels, timeout)
  }
}
