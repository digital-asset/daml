// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
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
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

class BlockedProgressDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  import BlockedProgressDetectorTest.*

  "BlockedProgressDetector" should {
    "detect blocked epoch progress" in {
      val epochStartBlockNumber = BlockNumber(1L)
      // the first block in the epoch is completed
      val completedBlocks = Seq(completedBlock(epochStartBlockNumber))
      val mySegment = createSegmentState(
        Segment(myId, NonEmpty(Seq, 1L, 3L, 5L).map(BlockNumber(_))),
        completedBlocks,
      )
      val otherSegment = Segment(otherId, NonEmpty(Seq, 2L, 4L, 6L).map(BlockNumber(_)))
      val mySegmentState = new LeaderSegmentState(mySegment, epoch, completedBlocks)
      val detector = new BlockedProgressDetector(
        epochStartBlockNumber,
        Some(mySegmentState),
        Map(myId -> mySegment.segment, otherId -> otherSegment),
        // this results in all previous blocks being completed
        isBlockComplete = _ => true,
      )

      detector.isProgressBlocked shouldBe true
    }

    "detect silent network" in {
      val epochStartBlockNumber = BlockNumber(1L)
      val mySegment = createSegmentState(Segment(myId, NonEmpty(Seq, epochStartBlockNumber)))
      val otherSegment = Segment(otherId, NonEmpty(Seq, BlockNumber(2L)))
      val detector = new BlockedProgressDetector(
        epochStartBlockNumber,
        Some(new LeaderSegmentState(mySegment, epoch, initialCompletedBlocks = Seq.empty)),
        Map(myId -> mySegment.segment, otherId -> otherSegment),
        isBlockComplete = _ => true,
      )

      detector.isProgressBlocked shouldBe false
      // ask again for the same state
      detector.isProgressBlocked shouldBe true
    }

    "not detect blocked progress when the current ordering node is not a leader" in {
      val detector = new BlockedProgressDetector(
        mySegmentState = None,
        leaderToSegmentState = Map.empty,
        epochStartBlockNumber = BlockNumber(1L),
        isBlockComplete = _ => true,
      )

      detector.isProgressBlocked shouldBe false
    }

    "not detect blocked progress when it's the first block in an epoch" in {
      val epochStartBlockNumber = BlockNumber(1L)
      val mySegment =
        createSegmentState(
          Segment(myId, NonEmpty(Seq, epochStartBlockNumber, 3L, 5L).map(BlockNumber(_)))
        )
      val otherSegment = Segment(otherId, NonEmpty(Seq, 2L, 4L, 6L).map(BlockNumber(_)))
      val detector = new BlockedProgressDetector(
        epochStartBlockNumber,
        // no blocks are completed
        Some(new LeaderSegmentState(mySegment, epoch, initialCompletedBlocks = Seq.empty)),
        Map(myId -> mySegment.segment, otherId -> otherSegment),
        isBlockComplete = _ => true,
      )

      detector.isProgressBlocked shouldBe false
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

  private def completedBlock(blockNumber: BlockNumber)(implicit
      synchronizerProtocolVersion: ProtocolVersion
  ) =
    Block(
      epochNumber,
      blockNumber,
      commitCertificate = CommitCertificate(
        PrePrepare
          .create(
            BlockMetadata(epochNumber, blockNumber),
            ViewNumber.First,
            OrderingBlock(Seq.empty),
            CanonicalCommitSet(Set.empty),
            myId,
          )
          .fakeSign,
        Seq.empty,
      ),
    )
}
