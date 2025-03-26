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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.DefaultMaxBatchesPerProposal
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisTopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

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
            OrderingBlock(Seq(createPoa())),
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
            OrderingBlock(Seq(createPoa())),
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
            OrderingBlock(Seq(createPoa())),
            CanonicalCommitSet.empty,
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "The canonical commit set is empty for block BlockMetadata(1,12) with 1 proofs of availability, " +
              "but it can only be empty for empty blocks or the first segment block"
          ),
        ),
        // negative: block has more batches than allowed
        (
          createPrePrepare(
            OrderingBlock(Seq.fill(DefaultMaxBatchesPerProposal.toInt + 1)(createPoa())),
            CanonicalCommitSet.empty,
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "The PrePrepare for block BlockMetadata(1,12) has 17 proofs of availability, but it should have up to " +
              "the maximum batch number per proposal of 16; the maximum batch number per proposal " +
              "should be configured with the same value across all nodes"
          ),
        ),
        // negative: block is not empty, but it's the first block of a segment in the first epoch
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
            CanonicalCommitSet.empty,
            BlockMetadata(EpochNumber.First, BlockNumber.First),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber.First)),
          aMembership,
          aMembership,
          Left(
            "The PrePrepare for block BlockMetadata(0,0) has 1 proofs of availability, but it should be empty"
          ),
        ),
        // positive: block is not empty, and it's the first epoch, but it's not the first block in the segment
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa(epochNumber = EpochNumber.First))),
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
            OrderingBlock(Seq(createPoa())),
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
          Left(
            "The canonical commit set refers to multiple epochs List(0, 1), but it should refer to just one"
          ),
        ),
        // negative: canonical commits contain different block numbers
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
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
            "The canonical commit set refers to multiple block numbers List(0, 10), but it should refer to just one"
          ),
        ),
        // negative: canonical commits contain a wrong epoch number
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
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
            "The canonical commit set refers to epoch number Some(1500) that is different from PrePrepare's epoch number 1"
          ),
        ),
        // negative: canonical commits contain a wrong epoch number at an epoch boundary
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
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
            "The canonical commit set for the first block in the segment refers to epoch number Some(1500) " +
              "that is different from PrePrepare's previous epoch number 0"
          ),
        ),
        // negative: canonical commits contain a wrong block number
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
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
            "The canonical commit set refers to block number Some(1500) " +
              "that is not the previous block number Some(10) " +
              "in the current segment List(10, 12)"
          ),
        ),
        // negative: canonical commits contain a wrong block number at an epoch boundary
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
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
            "The canonical commit set for the first block in the segment refers to block number Some(1500) " +
              "that is not the last block number from the previous epoch (11)"
          ),
        ),
        // negative: canonical commits (from the previous epoch) do not make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          Membership.forTesting(myId, Set(otherId)),
          Left(
            "The canonical commit set for block BlockMetadata(1,12) has size 1 " +
              "which is below the strong quorum of 2 for current topology Set(otherId, self)"
          ),
        ),
        // positive: canonical commits (from the previous epoch) make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
            CanonicalCommitSet(
              Set(
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber(12L)), myId),
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber(12L)), otherId),
              )
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber(13L))),
          Membership.forTesting(myId, Set(otherId)),
          aMembership,
          Right(()),
        ),
        // negative: canonical commits (from the current epoch) do not make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
            CanonicalCommitSet(
              Set(createCommit(BlockMetadata(EpochNumber(1L), BlockNumber(12L))))
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber(12L), BlockNumber(13L))),
          aMembership,
          Membership.forTesting(myId, Set(otherId)),
          Left(
            "The canonical commit set for block BlockMetadata(1,13) has size 1 " +
              "which is below the strong quorum of 2 for current topology Set(otherId, self)"
          ),
        ),
        // negative: canonical commits contain a commit from a wrong node
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
            CanonicalCommitSet(
              Set(createCommit(BlockMetadata(EpochNumber(1L), BlockNumber(12L)), from = otherId))
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber(12L), BlockNumber(13L))),
          aMembership,
          Membership.forTesting(myId),
          Left(
            "The canonical commit set for block BlockMetadata(1,13) contains a commit from 'otherId' " +
              "that is not part of current topology Set(self)"
          ),
        ),
        // positive: canonical commits (from the current epoch) make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
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
          Membership.forTesting(myId, Set(otherId)),
          Right(()),
        ),
        // negative: canonical commits' senders are not distinct
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa())),
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
          Membership.forTesting(myId, Set(otherId)),
          Left(
            "The canonical commit set for block BlockMetadata(1,13) contains duplicate senders: List(otherId, otherId)"
          ),
        ),
        // negative: non-empty block needs availability acks
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa(acks = Seq.empty))),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "The proof of availability for batch BatchId(SHA-256:d624dc8a1022...) " +
              "in PrePrepare for block BlockMetadata(1,12) " +
              "has 0 dissemination acknowledgements, but it should have at least 1"
          ),
        ),
        // negative: don't allow an expired poa
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa(epochNumber = EpochNumber(5L)))),
            CanonicalCommitSet(Set(createCommit())),
            BlockMetadata(
              EpochNumber(5L + OrderingRequestBatch.BatchValidityDurationEpochs),
              BlockNumber(100),
            ),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            s"The PrePrepare for block BlockMetadata(${5L + OrderingRequestBatch.BatchValidityDurationEpochs},100) " +
              s"has an expired proof of availability at 5, " +
              s"which is ${OrderingRequestBatch.BatchValidityDurationEpochs} epochs or more " +
              s"older than current epoch ${5L + OrderingRequestBatch.BatchValidityDurationEpochs}."
          ),
        ),
        // negative: don't allow a poa from a future epoch
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa(epochNumber = EpochNumber(6L)))),
            CanonicalCommitSet(Set(createCommit())),
            BlockMetadata(
              EpochNumber(5L),
              BlockNumber(100),
            ),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "The PrePrepare for block BlockMetadata(5,100) has a proof of availability for future epoch 6 (current epoch is 5)"
          ),
        ),
        // positive: poa just one epoch away from expiration is good
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa(epochNumber = EpochNumber(5L)))),
            CanonicalCommitSet(
              Set(
                createCommit(
                  BlockMetadata(
                    EpochNumber(5L + OrderingRequestBatch.BatchValidityDurationEpochs - 1L),
                    BlockNumber(99L),
                  )
                )
              )
            ),
            BlockMetadata(
              EpochNumber(5L + OrderingRequestBatch.BatchValidityDurationEpochs - 1L),
              BlockNumber(100),
            ),
          ),
          Segment(myId, NonEmpty(Seq, BlockNumber(99L), BlockNumber(100L))),
          aMembership,
          aMembership,
          Right(()),
        ),
        // negative: don't allow same sequencer to have multiple acks
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa(acks = Seq(createAck(myId), createAck(myId))))),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "The proof of availability for batch BatchId(SHA-256:d624dc8a1022...) " +
              "in PrePrepare for block BlockMetadata(1,12) has duplicated dissemination acknowledgements"
          ),
        ),
        // negative: ack from sequencer not in topology
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa(acks = Seq(createAck(otherId))))),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          aMembership,
          Left(
            "The dissemination acknowledgement for batch BatchId(SHA-256:d624dc8a1022...) from 'otherId' is invalid " +
              "because 'otherId' is not in the current topology (epoch 1, nodes Set(self))"
          ),
        ),
        // negative: ack with key not in topology
        (
          createPrePrepare(
            OrderingBlock(Seq(createPoa(acks = Seq(createAck(myId))))),
            CanonicalCommitSet(Set(createCommit())),
          ),
          aSegment,
          aMembership,
          aMembershipWithoutKeys,
          Left(
            "The dissemination acknowledgement for batch BatchId(SHA-256:d624dc8a1022...) from 'self' is invalid " +
              "because the signing key 'no-fingerprint' is not valid for 'self' in the current topology " +
              "(epoch 1, nodes Set(self)); the keys valid for 'self' in the current topology are Set()"
          ),
        ),
      ).forEvery { (prePrepare, segment, previousMembership, currentMembership, expectedResult) =>
        implicit val metricsContext: MetricsContext = MetricsContext.Empty
        implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig()

        val epoch = createEpoch(
          prePrepare.blockMetadata.epochNumber,
          segment.firstBlockNumber,
          currentMembership,
          previousMembership,
        )
        val validator =
          new PbftMessageValidatorImpl(
            segment,
            epoch,
            SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
          )(fail(_))

        validator.validatePrePrepare(prePrepare) shouldBe expectedResult
      }
    }
  }
}

object PbftMessageValidatorImplTest {
  private val myId = BftNodeId("self")
  private val otherId = BftNodeId("otherId")

  private val aBlockNumber: BlockNumber = BlockNumber(12L)
  private val aBlockMetadata = BlockMetadata(EpochNumber(1L), aBlockNumber)
  private val aPreviousBlockNumberInSegment: BlockNumber = BlockNumber(10L)
  private val aPreviousBlockInSegmentMetadata =
    BlockMetadata(EpochNumber(1L), aPreviousBlockNumberInSegment)
  private val aSegment = Segment(myId, NonEmpty(Seq, aPreviousBlockNumberInSegment, aBlockNumber))

  private val aMembership = Membership.forTesting(myId)
  private val aMembershipWithoutKeys =
    Membership.forTesting(
      myId,
      nodesTopologyInfos = Map(
        myId -> NodeTopologyInfo(
          activationTime = TopologyActivationTime(CantonTimestamp.MinValue),
          keyIds = Set.empty,
        )
      ),
    )

  private val acksWithMeOnly = Seq(createAck(myId))

  private val previousEpochMaxBftTime =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))

  private def createCommit(
      blockMetadata: BlockMetadata = aPreviousBlockInSegmentMetadata,
      from: BftNodeId = myId,
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
        previousEpochMaxBftTime,
      ),
      currentMembership,
      previousMembership,
    )

  private def createAck(from: BftNodeId): AvailabilityAck =
    AvailabilityAck(from, Signature.noSignature)

  private def createPoa(
      batchId: BatchId = BatchId.createForTesting("test"),
      acks: Seq[AvailabilityAck] = acksWithMeOnly,
      epochNumber: EpochNumber = EpochNumber(1L),
  ) =
    ProofOfAvailability(batchId, acks, epochNumber)

}
