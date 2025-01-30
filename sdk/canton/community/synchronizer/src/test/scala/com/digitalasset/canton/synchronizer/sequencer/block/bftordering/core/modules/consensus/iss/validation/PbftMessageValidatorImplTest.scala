// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
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
          "first in segment",
          "previous membership",
          "current membership",
          "expected result",
        ),
        // positive: block is empty so canonical commit set can be empty
        (
          createPrePrepare(OrderingBlock.empty, CanonicalCommitSet.empty),
          false,
          aMembership,
          aMembership,
          Right(()),
        ),
        // positive: block is not empty, but it's the first block in the segment, so canonical commit set can be empty
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet.empty,
          ),
          true,
          aMembership,
          aMembership,
          Right(()),
        ),
        // positive: block is not empty, it's not the first block in the segment, and canonical commit set is not empty
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(Set(createCommit())),
          ),
          false,
          aMembership,
          aMembership,
          Right(()),
        ),
        // negative: block is not empty, and it's not the first block in the segment, so canonical commit set cannot be empty
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet.empty,
          ),
          false,
          aMembership,
          aMembership,
          Left(
            "Canonical commit set is empty for block BlockMetadata(1,10) with 1 proofs of availability, " +
              "but it can only be empty for empty blocks or first blocks in segments"
          ),
        ),
        // negative: block is not empty, but it's the first block of a segment in the first epoch
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet.empty,
            BlockMetadata(EpochNumber.First, BlockNumber.First),
          ),
          true,
          aMembership,
          aMembership,
          Left(
            "PrePrepare for block BlockMetadata(0,0) has 1 proofs of availability, but it should be empty"
          ),
        ),
        // positive: block is not empty, and it's the first epoch, but it's not the first block in the segment
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(Set(createCommit())),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          false,
          aMembership,
          aMembership,
          Right(()),
        ),
        // negative: canonical commits contain different epochs
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(
              Set(
                createCommit(from = myId),
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber.First), otherId),
              )
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          false,
          aMembership,
          aMembership,
          Left("Canonical commits contain different epochs List(0, 1), should contain just one"),
        ),
        // negative: canonical commits (from the previous epoch) do not make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(
              Set(createCommit(BlockMetadata(EpochNumber.First, BlockNumber.First)))
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          false,
          Membership(myId, Set(otherId)),
          aMembership,
          Left("Canonical commit set has size 1 which is below the strong quorum of 2"),
        ),
        // positive: canonical commits (from the previous epoch) make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(
              Set(
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber.First), myId),
                createCommit(BlockMetadata(EpochNumber.First, BlockNumber.First), otherId),
              )
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          false,
          Membership(myId, Set(otherId)),
          aMembership,
          Right(()),
        ),
        // negative: canonical commits (from the current epoch) do not make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(
              Set(createCommit(BlockMetadata(EpochNumber(1L), BlockNumber(12L))))
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          false,
          aMembership,
          Membership(myId, Set(otherId)),
          Left("Canonical commit set has size 1 which is below the strong quorum of 2"),
        ),
        // positive: canonical commits (from the current epoch) make a strong quorum
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(
              Set(
                createCommit(BlockMetadata(EpochNumber(1L), BlockNumber(12L)), myId),
                createCommit(BlockMetadata(EpochNumber(1L), BlockNumber(12L)), otherId),
              )
            ),
            BlockMetadata(EpochNumber(1L), BlockNumber(13L)),
          ),
          false,
          aMembership,
          Membership(myId, Set(otherId)),
          Right(()),
        ),
      ).forEvery {
        (prePrepare, firstInSegment, previousMembership, currentMembership, expectedResult) =>
          val validator = new PbftMessageValidatorImpl(
            createEpoch(prePrepare, currentMembership, previousMembership),
            SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
          )(fail(_))(MetricsContext.Empty)

          validator.validatePrePrepare(prePrepare, firstInSegment) shouldBe expectedResult
      }
    }
  }
}

object PbftMessageValidatorImplTest {
  private val myId = fakeSequencerId("self")
  private val otherId = fakeSequencerId("otherId")

  private val aBlockMetadata = BlockMetadata(EpochNumber(1L), BlockNumber(10L))

  private val aMembership = Membership(myId)

  private def createCommit(
      blockMetadata: BlockMetadata = aBlockMetadata,
      from: SequencerId = myId,
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
        CantonTimestamp.Epoch,
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
      prePrepare: PrePrepare,
      currentMembership: Membership,
      previousMembership: Membership,
  ) =
    Epoch(
      EpochInfo(
        prePrepare.blockMetadata.epochNumber,
        BlockNumber.First, // ignored
        DefaultEpochLength, // ignored
        GenesisTopologyActivationTime, // ignored
      ),
      currentMembership,
      previousMembership,
      DefaultLeaderSelectionPolicy, // ignored
    )
}
