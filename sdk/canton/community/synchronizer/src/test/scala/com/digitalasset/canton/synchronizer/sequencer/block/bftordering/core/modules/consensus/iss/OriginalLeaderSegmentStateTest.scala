// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.*
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

class OriginalLeaderSegmentStateTest extends AsyncWordSpec with BftSequencerBaseTest {

  private val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
  private implicit val mc: MetricsContext = MetricsContext.Empty
  private implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig()
  private val clock = new SimClock(loggerFactory = loggerFactory)

  import OriginalLeaderSegmentStateTest.*

  "LeaderSegmentState" should {

    "assign blocks until all slots in segment are filled" in {
      val slots = NonEmpty.apply(Seq, 0L, 1L, 2L, 3L, 4L, 5L, 6L).map(BlockNumber(_))
      val segmentState = createSegmentState(slots)
      val leaderSegmentState =
        new OriginalLeaderSegmentState(segmentState, epoch, Seq.empty, Seq.empty, loggerFactory)
      // Emulate the first epoch behavior
      val initialCommits = Seq.empty

      slots.foreach { blockNumber =>
        leaderSegmentState.canReceiveProposals shouldBe true
        val orderedBlock = leaderSegmentState.assignToSlot(
          OrderingBlock.empty,
          latestCompletedEpochLastCommits = initialCommits,
        )
        completeBlock(segmentState, blockNumber)
        if (blockNumber == BlockNumber.First) {
          orderedBlock.canonicalCommitSet shouldBe CanonicalCommitSet(initialCommits.toSet)
        } else {
          val canonicalCommits = orderedBlock.canonicalCommitSet.sortedCommits
          canonicalCommits.size shouldBe 3
          canonicalCommits.foreach(_.message.blockMetadata.blockNumber shouldBe blockNumber - 1)
        }
      }

      leaderSegmentState.canReceiveProposals shouldBe false
      segmentState.isSegmentComplete shouldBe true
    }

    "restore state correctly after a restart" when {

      "some blocks are already completed and no blocks are in progress" in {
        val completedBlocks =
          Seq(BlockNumber.First, BlockNumber(2L)).map(n =>
            Block(
              EpochNumber.First,
              BlockNumber(n),
              CommitCertificate(
                PrePrepare
                  .create(
                    BlockMetadata(EpochNumber.First, n),
                    ViewNumber.First,
                    OrderingBlock.empty,
                    CanonicalCommitSet(Set.empty),
                    myId,
                  )
                  .fakeSign,
                commits,
              ),
            )
          )
        val segmentState =
          new SegmentState(
            segment = Segment(
              myId,
              NonEmpty.apply(Seq, 0L, 2L, 4L).map(BlockNumber(_)),
            ),
            epoch,
            clock,
            completedBlocks = completedBlocks,
            abort = fail(_),
            metrics,
            loggerFactory,
          )
        val leaderSegmentState =
          new OriginalLeaderSegmentState(
            segmentState,
            epoch,
            completedBlocks,
            initialCurrentViewPrePrepareBlockNumbers = Seq.empty,
            loggerFactory,
          )

        segmentState.isSegmentComplete shouldBe false
        leaderSegmentState.canReceiveProposals shouldBe true
        leaderSegmentState.nextBlockToPropose shouldBe 4L

        // Self has one slot left to assign (block=6); assign and verify
        val orderedBlock = leaderSegmentState.assignToSlot(OrderingBlock.empty, commits.take(1))
        orderedBlock.metadata.blockNumber shouldBe 4L
        orderedBlock.canonicalCommitSet.sortedCommits shouldBe commits
        leaderSegmentState.canReceiveProposals shouldBe false
      }

      "some blocks are already completed and a block is in progress" in {
        val completedBlocks =
          Seq(BlockNumber.First, BlockNumber(2L)).map(n =>
            Block(
              EpochNumber.First,
              BlockNumber(n),
              CommitCertificate(
                PrePrepare
                  .create(
                    BlockMetadata(EpochNumber.First, n),
                    ViewNumber.First,
                    OrderingBlock.empty,
                    CanonicalCommitSet(Set.empty),
                    myId,
                  )
                  .fakeSign,
                commits,
              ),
            )
          )
        val currentViewPrePrepareBlockNumbers = Seq(BlockNumber(4L))
        val segmentState =
          new SegmentState(
            segment = Segment(
              myId,
              NonEmpty.apply(Seq, 0L, 2L, 4L, 6L).map(BlockNumber(_)),
            ),
            epoch,
            clock,
            completedBlocks = completedBlocks,
            abort = fail(_),
            metrics,
            loggerFactory,
          )
        val leaderSegmentState =
          new OriginalLeaderSegmentState(
            segmentState,
            epoch,
            completedBlocks,
            currentViewPrePrepareBlockNumbers,
            loggerFactory,
          )

        segmentState.isSegmentComplete shouldBe false
        leaderSegmentState.canReceiveProposals shouldBe false // Because the current block is not complete
        leaderSegmentState.nextBlockToPropose shouldBe 6L
      }
    }

    "assign block with empty canonical commit set just after genesis" in {
      val segmentState =
        new SegmentState(
          segment = Segment(myId, NonEmpty.apply(Seq, 0L, 2L, 4L, 6L).map(BlockNumber(_))),
          epoch,
          clock,
          completedBlocks = Seq.empty,
          abort = fail(_),
          metrics,
          loggerFactory,
        )
      val leaderSegmentState =
        new OriginalLeaderSegmentState(segmentState, epoch, Seq.empty, Seq.empty, loggerFactory)

      val orderedBlock = leaderSegmentState.assignToSlot(
        OrderingBlock.empty,
        latestCompletedEpochLastCommits = GenesisEpoch.lastBlockCommits,
      )

      orderedBlock.canonicalCommitSet shouldBe CanonicalCommitSet.empty
    }

    "tell when this node is blocking progress" in {
      val membership = Membership.forTesting(myId, otherIds)
      val epoch = Epoch(
        EpochInfo.mk(
          number = EpochNumber.First,
          startBlockNumber = BlockNumber.First,
          length = 12,
        ),
        currentMembership = membership,
        previousMembership = membership, // Not relevant for the test
      )
      val mySegment =
        epoch.segments.find(_.originalLeader == myId).getOrElse(fail("myId should have a segment"))

      val segmentState =
        new SegmentState(
          segment = mySegment,
          epoch,
          clock,
          completedBlocks = Seq.empty,
          abort = fail(_),
          metrics,
          loggerFactory,
        )
      val leaderSegmentState =
        new OriginalLeaderSegmentState(segmentState, epoch, Seq.empty, Seq.empty, loggerFactory)

      val anotherNodeSegment =
        epoch.segments
          .find(_.originalLeader != myId)
          .getOrElse(fail("there should be more than one segment"))

      (0 until mySegment.slotNumbers.size).foreach { relativeIndex =>
        leaderSegmentState.isProgressBlocked shouldBe false

        leaderSegmentState.confirmCompleteBlockStored(
          anotherNodeSegment.slotNumbers(relativeIndex),
          isEmpty = false,
        )

        leaderSegmentState.isProgressBlocked shouldBe true
        val orderedBlock = leaderSegmentState.assignToSlot(OrderingBlock.empty, Seq.empty)
        completeBlock(segmentState, orderedBlock.metadata.blockNumber)
      }
      succeed
    }
  }

  private def createSegmentState(
      slots: NonEmpty[Seq[BlockNumber]],
      completedBlocks: Seq[Block] = Seq.empty,
  ) =
    new SegmentState(
      segment = Segment(myId, slots),
      epoch,
      clock,
      completedBlocks = completedBlocks,
      abort = fail(_),
      metrics,
      loggerFactory,
    )

  private def completeBlock(segmentState: SegmentState, blockNumber: BlockNumber): Unit = {
    val metadata = BlockMetadata.mk(0, blockNumber)
    val prePrepare =
      PrePrepare
        .create(
          metadata,
          ViewNumber.First,
          OrderingBlock.empty,
          CanonicalCommitSet.empty,
          from = segmentState.segment.originalLeader,
        )
        .fakeSign
    val ppHash = prePrepare.message.hash
    val _ = assertNoLogs(segmentState.processEvent(PbftSignedNetworkMessage(prePrepare)))
    segmentState.processEvent(prePrepare.message.stored)

    otherIds.foreach { node =>
      val prepare =
        Prepare
          .create(metadata, ViewNumber.First, ppHash, from = node)
          .fakeSign
      val _ = assertNoLogs(segmentState.processEvent(PbftSignedNetworkMessage(prepare)))
    }
    segmentState.processEvent(PreparesStored(metadata, ViewNumber.First))

    otherIds.foreach { node =>
      val commit =
        Commit
          .create(metadata, ViewNumber.First, ppHash, CantonTimestamp.Epoch, from = node)
          .fakeSign
      val _ = assertNoLogs(segmentState.processEvent(PbftSignedNetworkMessage(commit)))
    }
    segmentState.processEvent(prePrepare.message.stored)
    segmentState.processEvent(PreparesStored(metadata, ViewNumber.First))
    segmentState.isBlockComplete(blockNumber) shouldBe false
    segmentState.confirmCompleteBlockStored(blockNumber)
    segmentState.isBlockComplete(blockNumber) shouldBe true
  }
}

object OriginalLeaderSegmentStateTest {

  private val myId = BftNodeId("self")
  private val otherIds = (1 to 3).map { index =>
    BftNodeId(s"node$index")
  }.toSet
  private val currentMembership = Membership.forTesting(myId, otherIds)
  private val epoch =
    Epoch(
      EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, 7),
      currentMembership,
      previousMembership = currentMembership, // not relevant
    )

  private def commits(implicit synchronizerProtocolVersion: ProtocolVersion) = (otherIds + myId)
    .map { node =>
      Commit
        .create(
          BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
          ViewNumber.First,
          Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
          CantonTimestamp.Epoch,
          from = node,
        )
        .fakeSign
    }
    .toSeq
    .sorted
}
