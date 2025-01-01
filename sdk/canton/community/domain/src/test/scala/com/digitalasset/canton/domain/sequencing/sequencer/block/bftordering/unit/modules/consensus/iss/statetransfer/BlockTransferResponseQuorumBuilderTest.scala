// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.BlockTransferResponseQuorumBuilder
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.BlockTransferResponse
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AnyWordSpec

class BlockTransferResponseQuorumBuilderTest extends AnyWordSpec with BftSequencerBaseTest {

  import BlockTransferResponseQuorumBuilderTest.*

  "BlockTransferResponseQuorumBuilder" should {
    "build a quorum if possible" in {
      forAll(
        Table[Seq[BlockTransferResponse], Option[Set[BlockTransferResponse]]](
          ("collected responses", "expected potential quorum"),
          (
            Seq.empty,
            None,
          ),
          // There's a response, but there's no quorum
          (
            Seq(blockTransferResponseUpTo(epoch = 2L)),
            None,
          ),
          // Responses do not match
          (
            Seq(
              blockTransferResponseUpTo(
                epoch = 3L,
                timestampForPrePrepares = CantonTimestamp.Epoch,
              ),
              blockTransferResponseUpTo(
                epoch = 3L,
                from = anotherSequencerId,
                timestampForPrePrepares = CantonTimestamp.MaxValue,
              ),
            ),
            None,
          ),
          // Responses match even though they are up to different epochs
          (
            Seq(
              blockTransferResponseUpTo(epoch = 2L),
              blockTransferResponseUpTo(epoch = 3L),
            ),
            Some(
              Set(
                blockTransferResponseUpTo(epoch = 3L),
                blockTransferResponseUpTo(epoch = 2L),
              )
            ),
          ),
        )
      ) { (collectedResponses, expectedPotentialQuorum) =>
        val builder = new BlockTransferResponseQuorumBuilder(membership)
        collectedResponses.foreach(builder.addResponse)
        builder.build shouldBe expectedPotentialQuorum
      }
    }
  }
}

object BlockTransferResponseQuorumBuilderTest {

  private val aSequencerId = fakeSequencerId("aSequencer")
  private val anotherSequencerId = fakeSequencerId("anotherSequencer")

  // f = 1; f + 1 = 2
  private val membership =
    Membership(
      aSequencerId,
      Set(
        anotherSequencerId,
        fakeSequencerId("otherSequencer3"),
        fakeSequencerId("otherSequencer4"),
      ),
    )

  private def blockTransferResponseUpTo(
      epoch: Long,
      from: SequencerId = aSequencerId,
      timestampForPrePrepares: CantonTimestamp = CantonTimestamp.MinValue,
  ) =
    BlockTransferResponse.create(
      EpochNumber(epoch),
      (0L to epoch).map(e =>
        CommitCertificate(
          PrePrepare
            .create(
              // Assume epoch length = 1
              BlockMetadata(EpochNumber(e), BlockNumber(e)),
              ViewNumber.First,
              timestampForPrePrepares,
              OrderingBlock(Seq.empty),
              CanonicalCommitSet.empty,
              aSequencerId,
            )
            .fakeSign,
          // TODO(#22898): Test commits once they are taken into account
          commits = Seq.empty,
        )
      ),
      from,
    )
}
