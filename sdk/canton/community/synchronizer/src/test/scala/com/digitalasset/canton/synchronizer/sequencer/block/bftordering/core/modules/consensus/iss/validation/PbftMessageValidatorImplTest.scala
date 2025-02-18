// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.{
  DefaultEpochLength,
  DefaultLeaderSelectionPolicy,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisTopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class PbftMessageValidatorImplTest extends AnyWordSpec with BftSequencerBaseTest {

  import PbftMessageValidatorImplTest.*

  "validatePrePrepare" should {
    "work as expected" in {
      Table(
        (
          "pre-prepare",
          "segment",
          "previous membership",
          "current membership",
          "expected result",
        ),
        // positive: block is empty so canonical commit set can be empty
        (
          createPrePrepare(OrderingBlock.empty, CanonicalCommitSet.empty),
          aSegment,
          aMembership,
          aMembership,
          Right(()),
        ),
        // positive: block is not empty, but it's the first block in the segment, so canonical commit set can be empty
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet.empty,
            aBlockMetadata,
          ),
          Segment(myId, NonEmpty(Seq, aBlockMetadata.blockNumber)),
          aMembership,
          aMembership,
          Right(()),
        ),
        // positive: block is not empty, it's not the first block in the segment, and canonical commit set is not empty
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          aMembership,
          Right(()),
        ),
        // negative: block is not empty, and it's not the first block in the segment, so canonical commit set cannot be empty
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet.empty,
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "Canonical commit set is empty for block BlockMetadata(1,12) with 1 proofs of availability, " +
              "but it can only be empty for empty blocks or first blocks in segments"
          ),
        ),
        // negative: block is not empty, but it's the first block of a segment in the first epoch
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet.empty,
            BlockMetadata(EpochNumber.First, BlockNumber.First),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber.First)),
          aMembership,
          aMembership,
          Left(
            "PrePrepare for block BlockMetadata(0,0) has 1 proofs of availability, but it should be empty"
          ),
        ),
        // positive: block is not empty, and it's the first epoch, but it's not the first block in the segment
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(createCommit(BlockMetadata(EpochNumber.First, BlockNumber.First)))
            ),
            BlockMetadata(EpochNumber.First, BlockNumber(2L)),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber.First, BlockNumber(2L))),
          aMembership,
          aMembership,
          Right(()),
        ),
        // negative: canonical commits contain different epochs
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(from = myId),
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber.First), otherId),
              )
            ),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left("Canonical commits contain different epochs List(0, 1), should contain just one"),
        ),
        // negative: canonical commits contain different block numbers
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(from = myId),
                createCommit(BlockMetadata(EpochNumber(1L), BlockNumber.First), otherId),
              )
            ),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "Canonical commits contain different block numbers List(0, 10), should contain just one"
          ),
        ),
        // negative: canonical commits contain a wrong epoch number
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(BlockMetadata(EpochNumber(1500L), BlockNumber.First), otherId)
              )
            ),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "Canonical commits contain epoch number Some(1500) that is different from PrePrepare's epoch number 1"
          ),
        ),
        // negative: canonical commits contain a wrong epoch number at an epoch boundary
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(
                  BlockMetadata(EpochNumber(1500L), aPreviousBlockNumberInSegment),
                  otherId,
                )
              )
            ),
          ),
          Segment(myId, NonEmpty(Seq, aBlockMetadata.blockNumber)),
          aMembership,
          aMembership,
          Left(
            "Canonical commits for the first block in the segment contain epoch number Some(1500) that is different " +
              "from PrePrepare's previous epoch number 0"
          ),
        ),
        // negative: canonical commits contain a wrong block number
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(BlockMetadata(aBlockMetadata.epochNumber, BlockNumber(1500L)), otherId)
              )
            ),
            aBlockMetadata,
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "Canonical commits contain block number Some(1500) that does not refer to the previous block (Some(10)) " +
              "in the current segment List(10, 12)"
          ),
        ),
        // negative: canonical commits contain a wrong block number at an epoch boundary
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber(1500L)), otherId)
              )
            ),
            aBlockMetadata,
          ),
          Segment(myId, NonEmpty(Seq, aBlockMetadata.blockNumber)),
          aMembership,
          aMembership,
          Left(
            "Canonical commits for the first block in the segment refer to block number Some(1500) that is not " +
              "the last block from the previous epoch (11)"
          ),
        ),
        // negative: canonical commits (from the previous epoch) do not make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          Membership(myId, Set(otherId)),
          Left("Canonical commit set has size 1 which is below the strong quorum of 2"),
        ),
        // positive: canonical commits (from the previous epoch) make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber(12L)), myId),
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber(12L)), otherId),
              )
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber(13L))),
          Membership(myId, Set(otherId)),
          aMembership,
          Right(()),
        ),
        // negative: canonical commits (from the current epoch) do not make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(createCommit(BlockMetadata(EpochNumber(1L), BlockNumber(12L))))
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber(12L), BlockNumber(13L))),
          aMembership,
          Membership(myId, Set(otherId)),
          Left("Canonical commit set has size 1 which is below the strong quorum of 2"),
        ),
        // positive: canonical commits (from the current epoch) make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(BlockMetadata(EpochNumber(1L), BlockNumber(12L)), myId),
                createCommit(BlockMetadata(EpochNumber(1L), BlockNumber(12L)), otherId),
              )
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber(12L), BlockNumber(13L))),
          aMembership,
          Membership(myId, Set(otherId)),
          Right(()),
        ),
        // negative: canonical commits' senders are not distinct
        (
          createPrePrepare(
            OrderingBlock(
              Seq(ProofOfAvailability(BatchId.createForTesting("test"), acksWithMeOnly))
            ),
            CanonicalCommitSet(
              Set(
                createCommit(
                  BlockMetadata(EpochNumber(1L), BlockNumber(12L)),
                  otherId,
                  CantonTimestamp.MinValue,
                ),
                createCommit(
                  BlockMetadata(EpochNumber(1L), BlockNumber(12L)),
                  otherId,
                  CantonTimestamp.MaxValue,
                ),
              )
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber(12L), BlockNumber(13L))),
          aMembership,
          Membership(myId, Set(otherId)),
          Left(
            "Canonical commits contain duplicate senders: List(SEQ::ns::fake_otherId, SEQ::ns::fake_otherId)"
          ),
        ),
        // negative: non-empty block needs availability acks
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "PrePrepare for block BlockMetadata(1,12) with proof of availability have only 0 acks but should have at least 1"
          ),
        ),
        // negative: don't allow same sequencer to have multiple acks
        (
          createPrePrepare(
            OrderingBlock(
              Seq(
                ProofOfAvailability(
                  BatchId.createForTesting("test"),
                  Seq(createAck(myId), createAck(myId)),
                )
              )
            ),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "PrePrepare for block BlockMetadata(1,12) with proof of availability have acks from same sequencer"
          ),
        ),
      ).forEvery { (prePrepare, segment, previousMembership, currentMembership, expectedResult) =>
        val epoch = createEpoch(
          prePrepare.blockMetadata.epochNumber,
          segment.firstBlockNumber,
          currentMembership,
          previousMembership,
        )
        val validator = new PbftMessageValidatorImpl(
          segment,
          epoch,
          SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
        )(fail(_))(MetricsContext.Empty)

        validator.validatePrePrepare(prePrepare) shouldBe expectedResult
      }
    }
  }
}

object PbftMessageValidatorImplTest {
  private val myId = fakeSequencerId("self")
  private val otherId = fakeSequencerId("otherId")

  private val aBlockNumber: BlockNumber = BlockNumber(12L)
  private val aBlockMetadata = BlockMetadata(EpochNumber(1L), aBlockNumber)
  private val aPreviousBlockNumberInSegment: BlockNumber = BlockNumber(10L)
  private val aPreviousBlockInSegmentMetadata =
    BlockMetadata(EpochNumber(1L), aPreviousBlockNumberInSegment)
  private val aSegment = Segment(myId, NonEmpty(Seq, aPreviousBlockNumberInSegment, aBlockNumber))

  private val aMembership = Membership(myId)

  private val acksWithMeOnly = Seq(createAck(myId))

  private def createCommit(
      blockMetadata: BlockMetadata = aPreviousBlockInSegmentMetadata,
      from: SequencerId = myId,
      localTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ) =
    Commit
      .create(
        blockMetadata,
        ViewNumber.First,
        Hash.digest(
          HashPurpose.BftOrderingPbftBlock,
          ByteString.EMPTY,
          HashAlgorithm.Sha256,
        ),
        localTimestamp,
        from,
      )
      .fakeSign

  private def createPrePrepare(
      orderingBlock: OrderingBlock,
      canonicalCommitSet: CanonicalCommitSet,
      blockMetadata: BlockMetadata = aBlockMetadata,
  ) =
    PrePrepare.create(
      blockMetadata,
      ViewNumber.First,
      CantonTimestamp.Epoch,
      orderingBlock,
      canonicalCommitSet,
      from = myId,
    )

  private def createEpoch(
      epochNumber: EpochNumber,
      startBlockNumber: BlockNumber,
      currentMembership: Membership,
      previousMembership: Membership,
  ) =
    Epoch(
      EpochInfo(
        epochNumber,
        startBlockNumber,
        DefaultEpochLength, // ignored
        GenesisTopologyActivationTime, // ignored
      ),
      currentMembership,
      previousMembership,
      DefaultLeaderSelectionPolicy, // ignored
    )

  private def createAck(from: SequencerId): AvailabilityAck =
    AvailabilityAck(from, Signature.noSignature)
}
