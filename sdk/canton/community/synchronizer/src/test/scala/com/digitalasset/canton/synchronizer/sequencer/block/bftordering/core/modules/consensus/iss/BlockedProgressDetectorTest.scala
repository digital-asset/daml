// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.time.SimClock
import org.scalatest.wordspec.AnyWordSpec

class BlockedProgressDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  import BlockedProgressDetectorTest.*

  "BlockedProgressDetector" should {
    "detect blocked epoch progress when all other blocks are completed with non-empty" in {
      def isProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean =
        new BlockedProgressDetector(
          Map(myId -> mySegment, otherId -> otherSegment),
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
          Map(myId -> mySegment, otherId -> otherSegment),
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
          Map(myId -> mySegment, otherId -> otherSegment),
          isBlockComplete = blockNumber => otherSegment.slotNumbers.take(2).contains(blockNumber),
          isBlockEmpty = _ == otherSegment.slotNumbers(0),
        ).isProgressBlocked(nextRelativeBlockIndexToFill)

      isProgressBlocked(0) shouldBe true
    }

    "detect blocked epoch progress when another segment has completed, even with empty blocks" in {
      def isProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean =
        new BlockedProgressDetector(
          Map(myId -> mySegment, otherId -> otherSegment),
          isBlockComplete = blockNumber => otherSegment.slotNumbers.contains(blockNumber),
          isBlockEmpty = _ => true,
        ).isProgressBlocked(nextRelativeBlockIndexToFill)

      isProgressBlocked(0) shouldBe true
      isProgressBlocked(1) shouldBe true
      isProgressBlocked(2) shouldBe true
    }

    "detect silent network" in {
      val epochStartBlockNumber = BlockNumber(1L)
      val mySegment = createSegmentState(Segment(myId, NonEmpty(Seq, epochStartBlockNumber)))
      val otherSegment = Segment(otherId, NonEmpty(Seq, BlockNumber(2L)))
      val detector = new BlockedProgressDetector(
        Map(myId -> mySegment.segment, otherId -> otherSegment),
        isBlockComplete = _ => false,
        isBlockEmpty = _ => false,
      )

      detector.isProgressBlocked(0) shouldBe false
      // ask again for the same state
      detector.isProgressBlocked(0) shouldBe true
    }
  }

  private def createSegmentState(
      segment: Segment,
      completedBlocks: Seq[Block] = Seq.empty,
  ) = {
    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig()
    new SegmentState(
      segment,
      epoch,
      new SimClock(loggerFactory = loggerFactory),
      completedBlocks,
      abort = fail(_),
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      loggerFactory,
    )
  }
}

object BlockedProgressDetectorTest {

  private val myId = BftNodeId("self")
  private val otherId = BftNodeId("otherId")
  private val membership = Membership.forTesting(myId, Set(otherId))
  private val epochNumber = EpochNumber.First
  private val epoch = Epoch(
    EpochInfo.mk(epochNumber, startBlockNumber = BlockNumber.First, 7L),
    currentMembership = membership,
    previousMembership = membership, // Not relevant for the test
  )
  private val mySegment = Segment(myId, NonEmpty(Seq, 1L, 3L, 5L).map(BlockNumber(_)))
  private val otherSegment = Segment(otherId, NonEmpty(Seq, 2L, 4L, 6L).map(BlockNumber(_)))
}
