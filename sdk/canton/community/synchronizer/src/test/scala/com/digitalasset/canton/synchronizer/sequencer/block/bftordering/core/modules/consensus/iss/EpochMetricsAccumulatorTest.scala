// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import org.scalatest.wordspec.AsyncWordSpec

class EpochMetricsAccumulatorTest extends AsyncWordSpec with BaseTest {
  private val node1 = BftNodeId("sequencer1")
  private val node2 = BftNodeId("sequencer2")
  private val node3 = BftNodeId("sequencer3")

  "EpochMetricsAccumulator" should {
    "accumulate votes and views" in {
      val accumulator = new EpochMetricsAccumulator()

      accumulator.accumulate(3, Map(node1 -> 3), Map(node1 -> 2, node2 -> 2), 5)

      accumulator.viewsCount shouldBe 3
      accumulator.commitVotes shouldBe Map(node1 -> 3)
      accumulator.prepareVotes shouldBe Map(node1 -> 2, node2 -> 2)
      accumulator.discardedMessages shouldBe 5

      accumulator.accumulate(2, Map(node1 -> 2, node2 -> 2), Map(node3 -> 2, node2 -> 2), 10)

      accumulator.viewsCount shouldBe 5
      accumulator.commitVotes shouldBe Map(node1 -> 5, node2 -> 2)
      accumulator.prepareVotes shouldBe Map(node1 -> 2, node2 -> 4, node3 -> 2)
      accumulator.discardedMessages shouldBe 15
    }
  }
}
