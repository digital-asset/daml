// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusStatus,
}
import org.scalatest.wordspec.AnyWordSpec

class EpochStatusBuilderTest extends AnyWordSpec with BftSequencerBaseTest {
  val self = fakeSequencerId("self")
  val epoch0 = EpochNumber.First

  val completeSegment = ConsensusStatus.SegmentStatus.Complete
  val inViewChangeSegment =
    ConsensusStatus.SegmentStatus.InViewChange(ViewNumber.First, Seq.empty, Seq.empty)
  val inProgressSegment = ConsensusStatus.SegmentStatus.InProgress(ViewNumber.First, Seq.empty)

  "EpochStatusBuilder" should {
    "build epoch status based on segment index order" in {
      val epochStatusBuilder =
        new EpochStatusBuilder(self, epoch0, numberOfSegments = 3)

      epochStatusBuilder.epochStatus shouldBe empty

      epochStatusBuilder.receive(
        Consensus.RetransmissionsMessage.SegmentStatus(segmentIndex = 1, completeSegment)
      )
      epochStatusBuilder.epochStatus shouldBe empty

      epochStatusBuilder.receive(
        Consensus.RetransmissionsMessage.SegmentStatus(segmentIndex = 2, inViewChangeSegment)
      )
      epochStatusBuilder.epochStatus shouldBe empty

      epochStatusBuilder.receive(
        Consensus.RetransmissionsMessage.SegmentStatus(segmentIndex = 0, inProgressSegment)
      )
      epochStatusBuilder.epochStatus shouldBe Some(
        ConsensusStatus.EpochStatus.create(
          self,
          epoch0,
          Seq(inProgressSegment, completeSegment, inViewChangeSegment),
        )
      )
    }
  }
}
