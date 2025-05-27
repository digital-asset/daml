// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveDouble}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.time.SimClock
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class BftNodeRateLimiterTest extends AnyWordSpec with BftSequencerBaseTest {
  val node1 = BftNodeId("node1")
  val node2 = BftNodeId("node2")

  "BftNodeRateLimiter" should {
    // Note that the BftNodeRateLimiter uses the RateLimiter class, which is already widely tested.
    // So this test is just a much more basic one focusing just on the added elements on top, such as using
    // simulated clocks and rate limiting by node.
    "perform basic rate limiting per node with max burst" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(3)
      val maxTasksPerSecond = NonNegativeNumeric.tryCreate(1.toDouble)

      val limiter =
        new BftNodeRateLimiter(clock, maxTasksPerSecond, maxBurstFactor)

      // after initial max burst is reached, throttling of 1 task per second kicks in
      (0 until (maxBurstFactor.value.toInt)).foreach { _ =>
        limiter.checkAndUpdateRate(node1) shouldBe true
      }
      limiter.checkAndUpdateRate(node1) shouldBe false

      // node2 is tracked separately
      limiter.checkAndUpdateRate(node2) shouldBe true

      // after a bit of time, we can take the next task, but only 1
      clock.advance(Duration.ofMillis(1))
      limiter.checkAndUpdateRate(node1) shouldBe true
      limiter.checkAndUpdateRate(node1) shouldBe false

      // not enough time to take the next one
      clock.advance(Duration.ofMillis(1))
      limiter.checkAndUpdateRate(node1) shouldBe false

      // at the edge of being able to take the next one
      clock.advance(Duration.ofSeconds(1).minusMillis(2))
      limiter.checkAndUpdateRate(node1) shouldBe false

      // after a full second has passed, we can take the next
      clock.advance(Duration.ofMillis(1))
      limiter.checkAndUpdateRate(node1) shouldBe true
    }
  }
}
