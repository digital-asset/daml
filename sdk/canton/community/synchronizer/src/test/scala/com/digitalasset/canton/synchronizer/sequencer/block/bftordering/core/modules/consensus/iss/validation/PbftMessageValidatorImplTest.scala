// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.PbftMessageValidatorImpl
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class PbftMessageValidatorImplTest extends AnyWordSpec with BftSequencerBaseTest {

  import PbftMessageValidatorImplTest.*

  private val validator = new PbftMessageValidatorImpl(
    SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
  )(MetricsContext.Empty)

  "validatePrePrepare" should {
    "work as expected" in {
      Table(
        ("pre-prepare", "first in segment", "expected result"),
        // positive: block is empty so canonical commit set can be empty
        (
          createPrePrepare(OrderingBlock.empty, CanonicalCommitSet.empty),
          false,
          Right(()),
        ),
        // positive: block is not empty, but it's the first block in the segment, so canonical commit set can be empty
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet.empty,
          ),
          true,
          Right(()),
        ),
        // positive: block is not empty, it's not the first block in the segment, and canonical commit set is not empty
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(Set(aCommit)),
          ),
          false,
          Right(()),
        ),
        // negative: block is not empty, and it's not the first block in the segment, so canonical commit set cannot be empty
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet.empty,
          ),
          false,
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
          Left(
            "PrePrepare for block BlockMetadata(0,0) has 1 proofs of availability, but it should be empty"
          ),
        ),
        // positive: block is not empty, and it's the first epoch, but it's not the first block in the segment
        (
          createPrePrepare(
            OrderingBlock(Seq(ProofOfAvailability(BatchId.createForTesting("test"), Seq.empty))),
            CanonicalCommitSet(Set(aCommit)),
            BlockMetadata(EpochNumber.First, BlockNumber(3L)),
          ),
          false,
          Right(()),
        ),
      ).forEvery { (prePrepare, firstInSegment, expectedResult) =>
        validator.validatePrePrepare(prePrepare, firstInSegment) shouldBe expectedResult
      }
    }
  }
}

object PbftMessageValidatorImplTest {
  private val aSequencerId = fakeSequencerId("fake")

  private val aBlockMetadata = BlockMetadata(EpochNumber(1L), BlockNumber(10L))

  private val aCommit = Commit
    .create(
      aBlockMetadata,
      ViewNumber.First,
      Hash.digest(
        HashPurpose.BftOrderingPbftBlock,
        ByteString.EMPTY,
        HashAlgorithm.Sha256,
      ),
      CantonTimestamp.Epoch,
      aSequencerId,
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
      from = aSequencerId,
    )
}
