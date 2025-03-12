// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.Random

class SimpleLeaderSelectionPolicyTest extends AsyncWordSpec with BaseTest {

  import SimpleLeaderSelectionPolicyTest.*

  "SimpleLeaderSelectionPolicy" should {
    "return sorted leaders" in {
      val random = new Random(RandomSeed)
      val indexes: Seq[Int] = 0 until NumNodes
      val shuffledIndexes = random.shuffle(indexes)
      val nodes = shuffledIndexes.map { index =>
        BftNodeId(s"node$index")
      }
      val sortedNodes = nodes.sorted

      // Note that the seed is fixed and so are the nodes. The following assertions check the test itself.
      shuffledIndexes shouldNot be(indexes)
      nodes shouldNot be(sortedNodes)

      SimpleLeaderSelectionPolicy.selectLeaders(nodes.toSet).toSeq shouldBe sortedNodes
    }

    "rotate leaders" in {
      forAll(
        Table[Set[Int], Long, Seq[BftNodeId]](
          ("node indexes", "epoch number", "expected rotated leaders"),
          (Set(1), 13, Seq(BftNodeId(s"node1"))),
          (
            Set(1, 2, 3),
            0,
            Seq(
              BftNodeId("node1"),
              BftNodeId("node2"),
              BftNodeId("node3"),
            ),
          ),
          (
            Set(1, 2, 3),
            1,
            Seq(
              BftNodeId("node2"),
              BftNodeId("node3"),
              BftNodeId("node1"),
            ),
          ),
          (
            Set(1, 2, 3),
            2,
            Seq(
              BftNodeId("node3"),
              BftNodeId("node1"),
              BftNodeId("node2"),
            ),
          ),
          (
            Set(1, 2, 3),
            3,
            Seq(
              BftNodeId("node1"),
              BftNodeId("node2"),
              BftNodeId("node3"),
            ),
          ),
          (
            Set(1, 2, 3),
            4,
            Seq(
              BftNodeId("node2"),
              BftNodeId("node3"),
              BftNodeId("node1"),
            ),
          ),
        )
      ) { case (nodeIndexes, epochNumber, expectedRotatedLeaders) =>
        val nodes = nodeIndexes.map { index =>
          BftNodeId(s"node$index")
        }
        val selectedLeaders = SimpleLeaderSelectionPolicy.selectLeaders(nodes)
        val rotatedLeaders =
          LeaderSelectionPolicy.rotateLeaders(selectedLeaders, EpochNumber(epochNumber))

        rotatedLeaders should contain theSameElementsInOrderAs expectedRotatedLeaders
      }
    }
  }
}

object SimpleLeaderSelectionPolicyTest {
  private val NumNodes = 16
  private val RandomSeed = 4L
}
