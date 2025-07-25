// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import org.scalatest.wordspec.AsyncWordSpec

class EpochTest extends AsyncWordSpec with BftSequencerBaseTest {

  private val myId = BftNodeId("self")
  private val otherIds: Set[BftNodeId] = (1 to 3).map { index =>
    BftNodeId(s"node$index")
  }.toSet
  private val sortedLeaders = (otherIds + myId).toSeq.sorted

  "Epoch.segments" should {
    "support single leader" in {
      val membership = Membership.forTesting(myId)
      val epoch =
        Epoch(
          EpochInfo.mk(
            number = EpochNumber.First,
            startBlockNumber = BlockNumber.First,
            length = 4,
          ),
          currentMembership = membership,
          previousMembership = membership, // not relevant for this test
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
        currentMembership = Membership.forTesting(myId, otherIds),
        previousMembership = Membership.forTesting(myId),
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
      val membership = Membership.forTesting(leaderWithoutSegment, sortedLeaders.init.toSet)
      val epoch = Epoch(
        EpochInfo.mk(
          number = EpochNumber.First,
          startBlockNumber = BlockNumber.First,
          length = 3L, // epoch length can accommodate all but the last leader
        ),
        currentMembership = membership,
        previousMembership = membership, // not relevant for this test
      )
      // the last leader has no segment assigned to it
      epoch.segments.find(_.originalLeader == leaderWithoutSegment) shouldBe None
    }
  }
}
