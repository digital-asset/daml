// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import org.scalatest.wordspec.AsyncWordSpec

import OrderingTopology.{
  isStrongQuorumReached,
  isWeakQuorumReached,
  strongQuorumSize,
  weakQuorumSize,
}

class OrderingTopologyTest extends AsyncWordSpec with BaseTest {

  import OrderingTopologyTest.*

  "Utility quorum functions" should {
    "compute correct weak quorums" in {
      val results = List(1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6)
      results.zipWithIndex.foreach { case (result, idx) =>
        weakQuorumSize(idx + 1) shouldBe result
        isWeakQuorumReached(idx + 1, weakQuorumSize(idx + 1)) shouldBe true
      }
      succeed
    }

    "compute correct strong quorums" in {
      val results = List(1, 2, 3, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9, 10, 10, 11)
      results.zipWithIndex.foreach { case (result, idx) =>
        strongQuorumSize(idx + 1) shouldBe result
        isStrongQuorumReached(idx + 1, strongQuorumSize(idx + 1)) shouldBe true
      }
      succeed
    }
  }

  "Membership" should {
    "contain the correct size and quorum thresholds" in {
      forAll(
        Table(
          ("nodes", "total_size", "weak_quorum", "strong_quorum"),
          (Set.empty[BftNodeId], 1, 1, 1),
          (otherIds.take(1), 2, 1, 2),
          (otherIds.take(2), 3, 1, 3),
          (otherIds.take(3), 4, 2, 3),
          (otherIds.take(4), 5, 2, 4),
          (otherIds.take(5), 6, 2, 4),
          (otherIds.take(6), 7, 3, 5),
        )
      ) { (nodes, size, weakQuorum, strongQuorum) =>
        val topology = OrderingTopology.forTesting(nodes + myId)
        topology.nodes.size shouldBe size

        topology.weakQuorum shouldBe weakQuorum
        topology.hasWeakQuorum(weakQuorum) shouldBe true
        topology.hasWeakQuorum(weakQuorum + 1) shouldBe true
        topology.hasWeakQuorum(weakQuorum - 1) shouldBe false

        topology.strongQuorum shouldBe strongQuorum
        topology.hasStrongQuorum(strongQuorum) shouldBe true
        topology.hasStrongQuorum(strongQuorum + 1) shouldBe true
        topology.hasStrongQuorum(strongQuorum - 1) shouldBe false

        topology.contains(myId) shouldBe true
        topology.nodes.forall(topology.contains) shouldBe true
        topology.contains(BftNodeId("non-existent-node")) shouldBe false
      }
    }
  }
}

object OrderingTopologyTest {

  private val myId = BftNodeId("myId")
  private val otherIds = (1 to 6).map { index =>
    BftNodeId(s"node$index")
  }.toSet
}
