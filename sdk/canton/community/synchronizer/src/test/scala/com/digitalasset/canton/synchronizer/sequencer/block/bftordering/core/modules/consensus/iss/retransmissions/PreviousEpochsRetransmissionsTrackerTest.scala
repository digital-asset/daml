// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.{
  BlockStatus,
  SegmentStatus,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class PreviousEpochsRetransmissionsTrackerTest extends AnyWordSpec with BftSequencerBaseTest {
  val self = fakeSequencerId("self")
  val anotherPeer = fakeSequencerId("another")
  val epoch0 = EpochNumber.First
  val epoch1 = EpochNumber(epoch0 + 1)
  val wrongEpoch = EpochNumber(epoch0 + 1)

  val completeSegmentStatus = SegmentStatus.Complete

  def inProgressSegmentStatus(areBlocksComplete: Seq[Boolean]) = SegmentStatus
    .InProgress(
      ViewNumber.First,
      areBlocksComplete.map { completed =>
        if (completed) BlockStatus.Complete else BlockStatus.InProgress(false, Seq.empty, Seq.empty)
      },
    )

  def inViewChangeSegmentStatus(areBlocksComplete: Seq[Boolean]) =
    ConsensusStatus.SegmentStatus.InViewChange(ViewNumber.First, Seq.empty, areBlocksComplete)

  private val canonicalCommitSet = CanonicalCommitSet(
    Set(
      Commit
        .create(
          BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
          ViewNumber.First,
          Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
          CantonTimestamp.Epoch,
          from = self,
        )
        .fakeSign
    )
  )

  def createCommitCertificates(
      epochNumber: EpochNumber,
      numberOfBlocks: Int,
  ): Seq[CommitCertificate] =
    LazyList
      .from(0)
      .map(blockNumber =>
        CommitCertificate(
          PrePrepare
            .create(
              BlockMetadata.mk(epochNumber, BlockNumber(blockNumber.toLong)),
              ViewNumber.First,
              CantonTimestamp.Epoch,
              OrderingBlock(Seq()),
              canonicalCommitSet,
              from = self,
            )
            .fakeSign,
          Seq.empty,
        )
      )
      .take(numberOfBlocks)

  "PreviousEpochsRetransmissionsTracker" should {
    "reply with no commit certificates for epochs we have not yet completed" in {
      val tracker =
        new PreviousEpochsRetransmissionsTracker(howManyEpochsToKeep = 5, loggerFactory)

      tracker.processRetransmissionsRequest(
        ConsensusStatus.EpochStatus(
          anotherPeer,
          epoch0,
          Seq(
            inProgressSegmentStatus(Seq(true, false, false)),
            completeSegmentStatus,
            inViewChangeSegmentStatus(Seq(false, false, true)),
          ),
        )
      ) shouldBe empty
    }

    "retransmit commit certificates for incomplete blocks in previous epoch" in {
      val tracker =
        new PreviousEpochsRetransmissionsTracker(howManyEpochsToKeep = 5, loggerFactory)

      val commitCertificates = createCommitCertificates(epoch0, 10)

      tracker.endEpoch(epoch0, commitCertificates)

      tracker.processRetransmissionsRequest(
        ConsensusStatus.EpochStatus(
          anotherPeer,
          epoch0,
          Seq(
            inProgressSegmentStatus(Seq(false, true, false, false)), // blocks 0, 3, 6, 9
            completeSegmentStatus, // blocks 1, 4, 7
            SegmentStatus
              .InViewChange(ViewNumber.First, Seq.empty, Seq(true, false, false)), // blocks 2, 5, 8,
          ),
        )
      ) shouldBe Seq(
        commitCertificates(0),
        commitCertificates(5),
        commitCertificates(6),
        commitCertificates(8),
        commitCertificates(9),
      )
    }

    "purge epochs older than howManyEpochsToKeep" in {
      val howManyEpochsToKeep = 5
      val tracker =
        new PreviousEpochsRetransmissionsTracker(howManyEpochsToKeep, loggerFactory)

      val commitCertificates = createCommitCertificates(epoch0, 10)

      tracker.endEpoch(epoch0, commitCertificates)
      tracker.endEpoch(epoch1, createCommitCertificates(epoch1, 10))

      tracker.processRetransmissionsRequest(
        ConsensusStatus.EpochStatus(
          anotherPeer,
          epoch0,
          Seq(
            inProgressSegmentStatus(Seq(false, true, false, false, true)),
            inProgressSegmentStatus(Seq(false, true, false, false, false)),
          ),
        )
      ) should have size (7)

      val epochWhenFirstEpochGetsPurged = EpochNumber(epoch0 + howManyEpochsToKeep)
      tracker.endEpoch(
        epochWhenFirstEpochGetsPurged,
        createCommitCertificates(epochWhenFirstEpochGetsPurged, 10),
      )

      tracker.processRetransmissionsRequest(
        ConsensusStatus.EpochStatus(
          anotherPeer,
          epoch0,
          Seq(
            inProgressSegmentStatus(Seq(false, true, false, false, true)),
            inProgressSegmentStatus(Seq(false, true, false, false, false)),
          ),
        )
      ) shouldBe empty
    }
  }
}
