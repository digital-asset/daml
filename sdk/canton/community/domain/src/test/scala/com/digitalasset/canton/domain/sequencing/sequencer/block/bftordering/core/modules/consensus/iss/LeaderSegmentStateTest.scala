// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftSignedNetworkMessage,
  PrePrepare,
  Prepare,
  PreparesStored,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class LeaderSegmentStateTest extends AsyncWordSpec with BftSequencerBaseTest {

  private val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
  private implicit val mc: MetricsContext = MetricsContext.Empty
  private val clock = new SimClock(loggerFactory = loggerFactory)

  import LeaderSegmentStateTest.*

  "LeaderSegmentState" should {
    "assign blocks until all slots in segment are filled" in {
      val slots = NonEmpty.apply(Seq, BlockNumber.First, 1L, 2L, 3L, 4L, 5L, 6L).map(BlockNumber(_))
      val segmentState = createSegmentState(slots)
      val leaderSegmentState = new LeaderSegmentState(segmentState, epoch, Seq.empty)

      slots.foreach { blockNumber =>
        leaderSegmentState.moreSlotsToAssign shouldBe true
        val orderedBlock =
          leaderSegmentState.assignToSlot(
            OrderingBlock(Seq.empty),
            CantonTimestamp.now(),
            Seq.empty,
          )
        completeBlock(segmentState, blockNumber)
        val commits = orderedBlock.canonicalCommitSet.sortedCommits
        commits.size shouldBe 1
        commits.head.message.blockMetadata.blockNumber shouldBe blockNumber - 1
      }
      leaderSegmentState.moreSlotsToAssign shouldBe false

      segmentState.isSegmentComplete shouldBe true
    }

    "restore state correctly after a restart" in {
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
                  CantonTimestamp.Epoch,
                  OrderingBlock(Seq.empty),
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
          segment =
            Segment(myId, NonEmpty.apply(Seq, BlockNumber.First, 2L, 4L, 6L).map(BlockNumber(_))),
          epochNumber = EpochNumber.First,
          membership = Membership(myId),
          eligibleLeaders = Seq.empty,
          clock,
          completedBlocks = completedBlocks,
          abort = fail(_),
          metrics,
          loggerFactory,
        )
      val leaderSegmentState = new LeaderSegmentState(segmentState, epoch, completedBlocks)

      segmentState.isSegmentComplete shouldBe false
      leaderSegmentState.moreSlotsToAssign shouldBe true

      // Self has one slot left to assign (block=4); assign and verify
      val orderedBlock =
        leaderSegmentState
          .assignToSlot(OrderingBlock(Seq.empty), CantonTimestamp.now(), commits.take(1))
      orderedBlock.metadata.blockNumber shouldBe 4L
      orderedBlock.canonicalCommitSet.sortedCommits shouldBe commits
      leaderSegmentState.moreSlotsToAssign shouldBe false
    }

    "should assign block with provided timestamp in the canonical commit set at genesis" in {
      val segmentState =
        new SegmentState(
          segment =
            Segment(myId, NonEmpty.apply(Seq, BlockNumber.First, 2L, 4L, 6L).map(BlockNumber(_))),
          epochNumber = EpochNumber.First,
          membership = Membership(myId),
          eligibleLeaders = Seq.empty,
          clock,
          completedBlocks = Seq.empty,
          abort = fail(_),
          metrics,
          loggerFactory,
        )
      val leaderSegmentState = new LeaderSegmentState(segmentState, epoch, Seq.empty)

      val orderedBlock =
        leaderSegmentState.assignToSlot(OrderingBlock(Seq.empty), timestamp, Seq.empty)

      val canonicalCommits = orderedBlock.canonicalCommitSet.sortedCommits
      canonicalCommits.size shouldBe 1
      canonicalCommits.head.message.localTimestamp shouldBe timestamp
    }

    "tell when this node is blocking progress" in {
      val epoch = Epoch(
        EpochInfo.mk(
          number = EpochNumber.First,
          startBlockNumber = BlockNumber.First,
          length = 12,
        ),
        Membership(myId, otherPeers),
        SimpleLeaderSelectionPolicy,
      )
      val mySegment =
        epoch.segments.find(_.originalLeader == myId).getOrElse(fail("myId should have a segment"))

      val segmentState =
        new SegmentState(
          segment = mySegment,
          epochNumber = EpochNumber.First,
          membership = epoch.membership,
          eligibleLeaders = Seq.empty,
          clock,
          completedBlocks = Seq.empty,
          abort = fail(_),
          metrics,
          loggerFactory,
        )
      val leaderSegmentState = new LeaderSegmentState(segmentState, epoch, Seq.empty)
      mySegment.slotNumbers.foreach { slotNumber =>
        leaderSegmentState.isProgressBlocked shouldBe false
        (BlockNumber.First until slotNumber).foreach { n =>
          leaderSegmentState.confirmCompleteBlockStored(BlockNumber(n))
        }
        leaderSegmentState.isProgressBlocked shouldBe true
        val orderedBlock = leaderSegmentState.assignToSlot(
          OrderingBlock(Seq.empty),
          CantonTimestamp.now(),
          Seq.empty,
        )
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
      epochNumber = EpochNumber.First,
      membership = Membership(myId),
      eligibleLeaders = Seq.empty,
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
          CantonTimestamp.Epoch,
          OrderingBlock(Seq.empty),
          CanonicalCommitSet(Set.empty),
          from = segmentState.segment.originalLeader,
        )
        .fakeSign
    val ppHash = prePrepare.message.hash
    val _ = assertNoLogs(segmentState.processEvent(PbftSignedNetworkMessage(prePrepare)))
    otherPeers.foreach { peer =>
      val prepare =
        Prepare
          .create(metadata, ViewNumber.First, ppHash, CantonTimestamp.Epoch, from = peer)
          .fakeSign
      val _ = assertNoLogs(segmentState.processEvent(PbftSignedNetworkMessage(prepare)))

      val commit =
        Commit
          .create(metadata, ViewNumber.First, ppHash, CantonTimestamp.Epoch, from = peer)
          .fakeSign
      val _ = assertNoLogs(segmentState.processEvent(PbftSignedNetworkMessage(commit)))
    }
    segmentState.processEvent(prePrepare.message.stored)
    segmentState.processEvent(PreparesStored(metadata, ViewNumber.First))
    segmentState.isBlockComplete(blockNumber) shouldBe false
    segmentState.confirmCompleteBlockStored(blockNumber, ViewNumber.First)
    segmentState.isBlockComplete(blockNumber) shouldBe true
  }
}

object LeaderSegmentStateTest {

  private val timestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-02-16T12:00:00.000Z"))
  private val myId = fakeSequencerId("self")
  private val otherPeers: Set[SequencerId] = (1 to 3).map { index =>
    fakeSequencerId(s"peer$index")
  }.toSet
  private val currentMembership = Membership(myId, otherPeers)
  private val epoch =
    Epoch(
      EpochInfo.mk(EpochNumber.First, startBlockNumber = BlockNumber.First, 7),
      currentMembership,
      SimpleLeaderSelectionPolicy,
    )

  private val commits = (otherPeers + myId)
    .map { peer =>
      Commit
        .create(
          BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
          ViewNumber.First,
          Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
          CantonTimestamp.Epoch,
          from = peer,
        )
        .fakeSign
    }
    .toSeq
    .sorted
}
