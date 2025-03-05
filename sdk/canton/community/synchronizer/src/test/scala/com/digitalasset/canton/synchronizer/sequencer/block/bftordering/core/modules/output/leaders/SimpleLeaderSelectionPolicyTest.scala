// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.Random

class SimpleLeaderSelectionPolicyTest extends AsyncWordSpec with BaseTest {

  import SimpleLeaderSelectionPolicyTest.*

  "SimpleLeaderSelectionPolicy" should {
    "return sorted leaders" in {
      val random = new Random(RandomSeed)
      val indexes: Seq[Int] = 0 until NumPeers
      val shuffledIndexes = random.shuffle(indexes)
      val peers = shuffledIndexes.map { index =>
        fakeSequencerId(s"peer$index")
      }
      val sortedPeers = peers.sorted

      // Note that the seed is fixed and so are the peers. The following assertions check the test itself.
      shuffledIndexes shouldNot be(indexes)
      peers shouldNot be(sortedPeers)

      SimpleLeaderSelectionPolicy.selectLeaders(peers.toSet).toSeq shouldBe sortedPeers
    }

    "rotate leaders" in {
      forAll(
        Table[Set[Int], Long, Seq[SequencerId]](
          ("peer indexes", "epoch number", "expected rotated leaders"),
          (Set(1), 13, Seq(fakeSequencerId(s"peer1"))),
          (
            Set(1, 2, 3),
            0,
            Seq(
              fakeSequencerId("peer1"),
              fakeSequencerId("peer2"),
              fakeSequencerId("peer3"),
            ),
          ),
          (
            Set(1, 2, 3),
            1,
            Seq(
              fakeSequencerId("peer2"),
              fakeSequencerId("peer3"),
              fakeSequencerId("peer1"),
            ),
          ),
          (
            Set(1, 2, 3),
            2,
            Seq(
              fakeSequencerId("peer3"),
              fakeSequencerId("peer1"),
              fakeSequencerId("peer2"),
            ),
          ),
          (
            Set(1, 2, 3),
            3,
            Seq(
              fakeSequencerId("peer1"),
              fakeSequencerId("peer2"),
              fakeSequencerId("peer3"),
            ),
          ),
          (
            Set(1, 2, 3),
            4,
            Seq(
              fakeSequencerId("peer2"),
              fakeSequencerId("peer3"),
              fakeSequencerId("peer1"),
            ),
          ),
        )
      ) { case (peerIndexes, epochNumber, expectedRotatedLeaders) =>
        val peers = peerIndexes.map { index =>
          fakeSequencerId(s"peer$index")
        }
        val selectedLeaders = SimpleLeaderSelectionPolicy.selectLeaders(peers)
        val rotatedLeaders =
          LeaderSelectionPolicy.rotateLeaders(selectedLeaders, EpochNumber(epochNumber))

        rotatedLeaders should contain theSameElementsInOrderAs expectedRotatedLeaders
      }
    }
  }
}

object SimpleLeaderSelectionPolicyTest {
  private val NumPeers = 16
  private val RandomSeed = 4L
}
