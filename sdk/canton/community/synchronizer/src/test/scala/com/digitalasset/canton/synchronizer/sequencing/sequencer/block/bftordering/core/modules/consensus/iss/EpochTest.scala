// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AsyncWordSpec

class EpochTest extends AsyncWordSpec with BftSequencerBaseTest {

  private val myId = fakeSequencerId("self")
  private val otherPeers: Set[SequencerId] = (1 to 3).map { index =>
    fakeSequencerId(s"peer$index")
  }.toSet
  private val sortedLeaders = (otherPeers + myId).toSeq.sortWith((peer1, peer2) =>
    peer1.toProtoPrimitive < peer2.toProtoPrimitive
  )

  "Epoch.segments" should {
    "support single leader" in {
      val epoch =
        Epoch(
          EpochInfo.mk(
            number = EpochNumber.First,
            startBlockNumber = BlockNumber.First,
            length = 4,
          ),
          Membership(myId),
          SimpleLeaderSelectionPolicy,
        )
      epoch.segments should contain only Segment(
        myId,
        NonEmpty.mk(Seq, BlockNumber.First, 1L, 2L, 3L).map(BlockNumber(_)),
      )
    }

    "support an uneven distribution with multiple leaders" in {
      val epoch = Epoch(
        EpochInfo.mk(
          number = EpochNumber.First,
          startBlockNumber = BlockNumber.First,
          length = 11,
        ),
        Membership(myId, otherPeers),
        SimpleLeaderSelectionPolicy,
      )

      epoch.segments should contain theSameElementsInOrderAs List(
        Segment(
          sortedLeaders.head,
          NonEmpty.mk(Seq, BlockNumber.First, 4L, 8L).map(BlockNumber(_)),
        ),
        Segment(sortedLeaders(1), NonEmpty.mk(Seq, 1L, 5L, 9L).map(BlockNumber(_))),
        Segment(sortedLeaders(2), NonEmpty.mk(Seq, 2L, 6L, 10L).map(BlockNumber(_))),
        Segment(sortedLeaders(3), NonEmpty.mk(Seq, 3L, 7L).map(BlockNumber(_))),
      )
    }

    "handle nodes without a segment" in {
      val leaderWithoutSegment = sortedLeaders(3)
      val epoch = Epoch(
        EpochInfo.mk(
          number = EpochNumber.First,
          startBlockNumber = BlockNumber.First,
          length = 3L, // epoch length can accommodate all but the last leader
        ),
        Membership(leaderWithoutSegment, sortedLeaders.init.toSet),
        SimpleLeaderSelectionPolicy,
      )
      // the last leader has no segment assigned to it
      epoch.segments.find(_.originalLeader == leaderWithoutSegment) shouldBe None
    }
  }
}
