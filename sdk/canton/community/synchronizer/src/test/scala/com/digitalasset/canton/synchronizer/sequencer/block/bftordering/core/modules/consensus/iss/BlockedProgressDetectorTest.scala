// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}
import org.scalatest.wordspec.AnyWordSpec

class BlockedProgressDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  import BlockedProgressDetectorTest.*

  "BlockedProgressDetector" should {
    "detect blocked epoch progress when all other blocks are completed with non-empty" in {
      def isProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean =
        new BlockedProgressDetector(
          mySegment,
          Map(otherId -> otherSegment),
          // this results in all blocks being completed
          isBlockComplete = _ => true,
          isBlockEmpty = _ => false,
        ).isProgressBlocked(nextRelativeBlockIndexToFill)
      // we are blocking progress on all our slots because all other slots are filled with non-empty blocks
      isProgressBlocked(0) shouldBe true
      isProgressBlocked(1) shouldBe true
      isProgressBlocked(2) shouldBe true
    }

    "detect blocked epoch progress when block at same relative block index is complete with non-empty" in {
      def isProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean =
        new BlockedProgressDetector(
          mySegment,
          Map(otherId -> otherSegment),
          isBlockComplete = blockNumber => otherSegment.slotNumbers.take(2).contains(blockNumber),
          isBlockEmpty = _ == otherSegment.slotNumbers(1),
        ).isProgressBlocked(nextRelativeBlockIndexToFill)

      isProgressBlocked(0) shouldBe true
      isProgressBlocked(1) shouldBe false
      isProgressBlocked(2) shouldBe false
    }

    "detect blocked epoch progress when block at higher relative block index is complete with non-empty" in {
      def isProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean =
        new BlockedProgressDetector(
          mySegment,
          Map(otherId -> otherSegment),
          isBlockComplete = blockNumber => otherSegment.slotNumbers.take(2).contains(blockNumber),
          isBlockEmpty = _ == otherSegment.slotNumbers(0),
        ).isProgressBlocked(nextRelativeBlockIndexToFill)

      isProgressBlocked(0) shouldBe true
    }

    "detect blocked epoch progress when another segment has completed, even with empty blocks, only when we are at the last slot" in {
      def isProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean =
        new BlockedProgressDetector(
          mySegment,
          Map(otherId -> otherSegment),
          isBlockComplete = blockNumber => otherSegment.slotNumbers.contains(blockNumber),
          isBlockEmpty = _ => true,
        ).isProgressBlocked(nextRelativeBlockIndexToFill)

      isProgressBlocked(0) shouldBe false
      isProgressBlocked(1) shouldBe false
      isProgressBlocked(2) shouldBe true
    }
  }
}

object BlockedProgressDetectorTest {

  private val myId = BftNodeId("self")
  private val otherId = BftNodeId("otherId")
  private val mySegment = Segment(myId, NonEmpty(Seq, 1L, 3L, 5L).map(BlockNumber(_)))
  private val otherSegment = Segment(otherId, NonEmpty(Seq, 2L, 4L, 6L).map(BlockNumber(_)))
}
