// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime.MinimumBlockTimeGranularity
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.jdk.DurationConverters.*

class BftTimeTest extends AnyWordSpec with BftSequencerBaseTest {

  import BftTimeTest.*

  "BftTime" should {
    "calculate block BFT time" in {
      forAll(
        Table[CanonicalCommitSet, CantonTimestamp, CantonTimestamp](
          ("canonical commit set", "previous block BFT time", "expected BFT block time"),
          (
            // This is unrealistic because we always provide a canonical commit set (even for the genesis block).
            CanonicalCommitSet(Set.empty),
            CantonTimestamp.Epoch,
            CantonTimestamp.Epoch.add(MinimumBlockTimeGranularity.toJava),
          ),
          (
            CanonicalCommitSet(Set(createCommit(BaseTimestamp))),
            CantonTimestamp.Epoch,
            BaseTimestamp,
          ),
          (
            CanonicalCommitSet(Set(createCommit(BaseTimestamp))),
            BaseTimestamp,
            BaseTimestamp.add(MinimumBlockTimeGranularity.toJava),
          ),
          (
            CanonicalCommitSet(
              Set(
                createCommit(BaseTimestamp.immediateSuccessor, BftNodeId("node-1")),
                createCommit(BaseTimestamp.immediatePredecessor, BftNodeId("node-2")),
                createCommit(BaseTimestamp, BftNodeId("node-3")),
              )
            ),
            CantonTimestamp.Epoch,
            BaseTimestamp,
          ),
          (
            CanonicalCommitSet(
              Set(
                createCommit(BaseTimestamp.immediateSuccessor, BftNodeId("node-1")),
                createCommit(BaseTimestamp.immediatePredecessor, BftNodeId("node-2")),
                createCommit(
                  BaseTimestamp.immediateSuccessor.immediateSuccessor,
                  BftNodeId("node-3"),
                ),
                createCommit(BaseTimestamp, BftNodeId("node-4")),
              )
            ),
            CantonTimestamp.Epoch,
            BaseTimestamp.immediateSuccessor,
          ),
        )
      ) { case (canonicalCommitSet, previousBlockBftTime, expectedBftBlockTime) =>
        val blockTime = BftTime.blockBftTime(canonicalCommitSet, previousBlockBftTime)
        blockTime shouldBe expectedBftBlockTime
      }
    }

    "calculate request BFT time" in {
      forAll(
        Table[CantonTimestamp, Int, CantonTimestamp](
          ("block BFT time", "request index", "expected request BFT time"),
          (BaseTimestamp, 0, BaseTimestamp),
          (BaseTimestamp, 1, BaseTimestamp.add(BftTime.RequestTimeGranularity.toJava)),
          (BaseTimestamp, 10, BaseTimestamp.add((BftTime.RequestTimeGranularity * 10L).toJava)),
        )
      ) { case (blockBftTime, requestIndex, expectedRequestBftTime) =>
        val requestTime = BftTime.requestBftTime(blockBftTime, requestIndex)
        requestTime shouldBe expectedRequestBftTime
      }
    }
  }
}

object BftTimeTest {

  private val BaseTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-02-16T12:00:00.000Z"))

  private def createCommit(timestamp: CantonTimestamp, from: BftNodeId = BftNodeId.Empty)(implicit
      synchronizerProtocolVersion: ProtocolVersion
  ) =
    Commit
      .create(
        BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
        ViewNumber.First,
        Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
        timestamp,
        from,
      )
      .fakeSign
}
