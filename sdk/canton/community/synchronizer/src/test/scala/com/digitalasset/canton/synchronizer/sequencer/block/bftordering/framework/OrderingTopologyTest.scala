// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.topology.SequencerId
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

    "compute correct quorum probabilities" in {
      forAll(
        Table(
          (
            "previous topology",
            "current topology",
            "votes",
            "probability of dissemination success in new topology",
          ),
          // If the previous and current topology are the same, success is certain.
          (
            OrderingTopology(Set(fakeSequencerId("peer1"), fakeSequencerId("peer2"))),
            OrderingTopology(Set(fakeSequencerId("peer1"), fakeSequencerId("peer2"))),
            Set.empty[SequencerId],
            BigDecimal(1),
          ),
          // If the current topology includes the previous topology and the quorum size is the same, success is certain.
          (
            OrderingTopology(Set(fakeSequencerId("peer1"), fakeSequencerId("peer2"))),
            OrderingTopology(
              Set(fakeSequencerId("peer1"), fakeSequencerId("peer2"), fakeSequencerId("peer3"))
            ),
            Set.empty[SequencerId],
            BigDecimal(1),
          ),
          // If the current topology includes the previous topology and the quorum size is NOT the same,
          //  failure is certain.
          (
            OrderingTopology(Set(fakeSequencerId("peer1"), fakeSequencerId("peer2"))),
            OrderingTopology(
              Set(
                fakeSequencerId("peer1"),
                fakeSequencerId("peer2"),
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
              )
            ),
            Set.empty[SequencerId],
            BigDecimal(0),
          ),
          // If current and previous topologies are disjoint, failure is certain.
          (
            OrderingTopology(Set(fakeSequencerId("peer1"), fakeSequencerId("peer2"))),
            OrderingTopology(Set(fakeSequencerId("peer3"), fakeSequencerId("peer4"))),
            Set.empty[SequencerId],
            BigDecimal(0),
          ),
          // If current and previous topologies have both size 2, share 1 peer and only one vote is missing,
          //  the missing vote can come with equal probability either from the shared peer (success)
          //  or from the peer in the previous topology that is not shared (failure).
          (
            OrderingTopology(Set(fakeSequencerId("peer1"), fakeSequencerId("peer2"))),
            OrderingTopology(Set(fakeSequencerId("peer2"), fakeSequencerId("peer3"))),
            Set.empty[SequencerId],
            BigDecimal(1) / 2,
          ),
          // Like before but with the old topology having size 3: since the missing vote can also come
          //  from a further peer in the old topology that is not shared, the probability of success is now 1/3.
          (
            OrderingTopology(
              Set(fakeSequencerId("peer1"), fakeSequencerId("peer2"), fakeSequencerId("peer3"))
            ),
            OrderingTopology(Set(fakeSequencerId("peer3"), fakeSequencerId("peer4"))),
            Set.empty[SequencerId],
            BigDecimal(1) / 3,
          ),
          // Like before but with the old topology having size 4 and 2 votes needed; the possible outcomes are:
          //  [1, 2], [2, 1], [1, 3], [3, 1], [1, 4], [4, 1], [2, 3], [3, 2], [2, 4], [4, 2], [3, 4], [4, 3]
          //  while the favorable outcomes are:
          //  [1, 4], [4, 1], [2, 4], [4, 2], [3, 4], [4, 3]
          (
            OrderingTopology(
              Set(
                fakeSequencerId("peer1"),
                fakeSequencerId("peer2"),
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
              )
            ),
            OrderingTopology(Set(fakeSequencerId("peer4"), fakeSequencerId("peer5"))),
            Set.empty[SequencerId],
            BigDecimal(6) / 12,
          ),
          // Like before but with the old and new topologies sharing 2 peers; the possible outcomes are:
          //  [1, 2], [2, 1], [1, 3], [3, 1], [1, 4], [4, 1], [2, 3], [3, 2], [2, 4], [4, 2], [3, 4], [4, 3]
          //  while the favorable outcomes are:
          //  [3, 1], [3, 2], [4, 1], [4, 2], [1, 3], [1, 4], [2, 3], [2, 4], [3, 4], [4, 3]
          (
            OrderingTopology(
              Set(
                fakeSequencerId("peer1"),
                fakeSequencerId("peer2"),
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
              )
            ),
            OrderingTopology(
              Set(fakeSequencerId("peer3"), fakeSequencerId("peer4"), fakeSequencerId("peer5"))
            ),
            Set.empty[SequencerId],
            BigDecimal(10) / 12,
          ),
          // Previous and current topology have 4 nodes, we want for both quorum 2, and they share 2 peers;
          //  the possible outcomes are:
          //  [1, 2], [2, 1], [1, 3], [3, 1], [1, 4], [4, 1], [2, 3], [3, 2], [2, 4], [4, 2], [3, 4], [4, 3]
          //  while the favorable outcomes are:
          //  [3, 4], [4, 3]
          (
            OrderingTopology(
              Set(
                fakeSequencerId("peer1"),
                fakeSequencerId("peer2"),
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
              )
            ),
            OrderingTopology(
              Set(
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
                fakeSequencerId("peer5"),
                fakeSequencerId("peer6"),
              )
            ),
            Set.empty[SequencerId],
            BigDecimal(2) / 12,
          ),
          // Previous and current topology have 4 nodes, we want for both quorum 2, and they share 2 peers; also, one
          //  vote already came from a peer that is not shared. The possible outcomes are:
          //  [1, 3], [3, 1], [2, 3], [3, 2], [3, 4], [4, 3]
          //  while the favorable outcomes are:
          //  [3, 4], [4, 3]
          (
            OrderingTopology(
              Set(
                fakeSequencerId("peer1"),
                fakeSequencerId("peer2"),
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
              )
            ),
            OrderingTopology(
              Set(
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
                fakeSequencerId("peer5"),
                fakeSequencerId("peer6"),
              )
            ),
            Set(fakeSequencerId("peer3")),
            BigDecimal(1) / 3,
          ),
          // Previous and current topology have 4 nodes, we want for both quorum 2, and they share 2 peers but one
          //  vote went wasted on a peer that is not shared, i.e, the votes to go in the current topology are
          //  less than the votes missing for a quorum, so failure is certain.
          (
            OrderingTopology(
              Set(
                fakeSequencerId("peer1"),
                fakeSequencerId("peer2"),
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
              )
            ),
            OrderingTopology(
              Set(
                fakeSequencerId("peer3"),
                fakeSequencerId("peer4"),
                fakeSequencerId("peer5"),
                fakeSequencerId("peer6"),
              )
            ),
            Set(fakeSequencerId("peer1")),
            BigDecimal(0),
          ),
        )
      ) { (previousTopology, currentTopology, votes, probability) =>
        currentTopology.successProbabilityOfStaleDissemination(
          previousTopology,
          votes,
        ) shouldBe probability
      }
    }
  }

  "Membership" should {
    "contain the correct size and quorum thresholds" in {
      forAll(
        Table(
          ("peers", "total_size", "weak_quorum", "strong_quorum"),
          (Set.empty[SequencerId], 1, 1, 1),
          (otherPeers.take(1), 2, 1, 2),
          (otherPeers.take(2), 3, 1, 3),
          (otherPeers.take(3), 4, 2, 3),
          (otherPeers.take(4), 5, 2, 4),
          (otherPeers.take(5), 6, 2, 4),
          (otherPeers.take(6), 7, 3, 5),
        )
      ) { (peers, size, weakQuorum, strongQuorum) =>
        val topology = OrderingTopology(peers + myId)
        topology.peers.size shouldBe size

        topology.weakQuorum shouldBe weakQuorum
        topology.hasWeakQuorum(weakQuorum) shouldBe true
        topology.hasWeakQuorum(weakQuorum + 1) shouldBe true
        topology.hasWeakQuorum(weakQuorum - 1) shouldBe false

        topology.strongQuorum shouldBe strongQuorum
        topology.hasStrongQuorum(strongQuorum) shouldBe true
        topology.hasStrongQuorum(strongQuorum + 1) shouldBe true
        topology.hasStrongQuorum(strongQuorum - 1) shouldBe false

        topology.contains(myId) shouldBe true
        topology.peers.forall(topology.contains) shouldBe true
        topology.contains(fakeSequencerId("non-existent-peer")) shouldBe false
      }
    }
  }
}

object OrderingTopologyTest {

  private val myId = fakeSequencerId("myId")
  private val otherPeers = (1 to 6).map { index =>
    fakeSequencerId(
      s"peer$index"
    )
  }.toSet
}
