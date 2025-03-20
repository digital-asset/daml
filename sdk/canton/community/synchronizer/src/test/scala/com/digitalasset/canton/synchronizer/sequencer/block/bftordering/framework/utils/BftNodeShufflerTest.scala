// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.utils

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class BftNodeShufflerTest extends AnyWordSpec with BftSequencerBaseTest {

  private val random = new Random(4) // there's no better choice

  "BftNodeShuffler" should {
    "not blow up on an empty sequence" in {
      val shuffler = new BftNodeShuffler(random)
      val shuffled = shuffler.shuffle(Seq.empty)
      shuffled.headOption should not be defined
      shuffled.tail should be(empty)
    }

    "return a single node for a one-element sequence" in {
      val shuffler = new BftNodeShuffler(random)
      val node = BftNodeId("super-node")
      val shuffled = shuffler.shuffle(Seq(node))
      shuffled.headOption shouldBe Some(node)
      shuffled.tail should be(empty)
    }

    "can return different nodes each time for a longer sequence" in {
      val shuffler = new BftNodeShuffler(random)
      val nodes = Seq(BftNodeId("node1"), BftNodeId("node2"), BftNodeId("node3"))

      val shuffled1 = shuffler.shuffle(nodes)
      val shuffled2 = shuffler.shuffle(nodes)
      val shuffled3 = shuffler.shuffle(nodes)

      // a good candidate for property-based testing, but doing only basic checks,
      //  as we essentially test `random.shuffle` here
      shuffled1.headOption should contain oneElementOf nodes
      shuffled1.tail.size shouldBe 2
      shuffled1.tail.distinct.size shouldBe 2
      shuffled2.tail should not contain shuffled1.headOption
      shuffled2.tail should contain atLeastOneElementOf nodes

      // The purpose of the below checks is to show that, for a certain seed, nodes can be shuffled
      //  in a completely different order each time the shuffling happens (resulting in more even load balancing).
      //  It might not be the case for different seeds.
      shuffled1.headOption should not be shuffled2.headOption
      shuffled2.headOption should not be shuffled3.headOption
      shuffled3.headOption should not be shuffled1.headOption
      shuffled1.tail should not be shuffled2.tail
      shuffled2.tail should not be shuffled3.tail
      shuffled3.tail should not be shuffled1.tail
    }
  }
}
