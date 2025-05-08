// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.RetransmissionMessageValidator.RetransmissionResponseValidationError
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.RetransmissionsMessage.{
  RetransmissionRequest,
  RetransmissionResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.{
  BlockStatus,
  EpochStatus,
  SegmentStatus,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

class RetransmissionMessageValidatorTest extends AnyWordSpec with BftSequencerBaseTest {
  import RetransmissionMessageValidatorTest.*

  "RetransmissionMessageValidator.validateRetransmissionRequest" should {
    "error when the number of segments is not correct" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val request = retransmissionRequest(segments = Seq.empty)

      val result =
        validator.validateRetransmissionRequest(request)
      result shouldBe Left(
        "Got a retransmission request from node0 with 0 segments when there should be 1, ignoring"
      )
    }

    "error when all segments are complete" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val request = retransmissionRequest(segments = Seq(SegmentStatus.Complete))

      val result =
        validator.validateRetransmissionRequest(request)
      result shouldBe Left(
        "Got a retransmission request from node0 where all segments are complete so no need to process request, ignoring"
      )
    }

    "error when viewChangeMessagesPresent has wrong size in one of the segment statuses" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val request =
        retransmissionRequest(
          segments = Seq(SegmentStatus.InViewChange(ViewNumber.First, Seq.empty, Seq.empty))
        )

      val result =
        validator.validateRetransmissionRequest(request)
      result shouldBe Left(
        "Got a malformed retransmission request from node0 at segment 0, wrong size of view-change list, ignoring"
      )
    }

    "error when areBlocksComplete has wrong size in one of the segment statuses" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val request =
        retransmissionRequest(
          segments = Seq(SegmentStatus.InViewChange(ViewNumber.First, Seq(false), Seq.empty))
        )

      val result =
        validator.validateRetransmissionRequest(request)
      result shouldBe Left(
        "Got a malformed retransmission request from node0 at segment 0, wrong size of block completion list, ignoring"
      )
    }

    "validate correctly status with view change" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val request = retransmissionRequest(
        segments = Seq(
          SegmentStatus.InViewChange(
            ViewNumber.First,
            Seq(false),
            Seq.fill(epochLength.toInt)(false),
          )
        )
      )
      val result = validator.validateRetransmissionRequest(request)
      result shouldBe Right(())
    }

    "error when blockStatuses has wrong size in one of the segment statuses" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val request = retransmissionRequest(
        segments = Seq(
          SegmentStatus.InProgress(
            ViewNumber.First,
            Seq.empty,
          )
        )
      )
      val result = validator.validateRetransmissionRequest(request)
      result shouldBe Left(
        "Got a malformed retransmission request from node0 at segment 0, wrong size of blocks status list, ignoring"
      )
    }

    "error when pbft messages list has wrong size in one of the segment statuses" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val request = retransmissionRequest(
        segments = Seq(
          SegmentStatus.InProgress(
            ViewNumber.First,
            Seq.fill(epochLength.toInt)(BlockStatus.InProgress(false, Seq.empty, Seq.empty)),
          )
        )
      )
      val result = validator.validateRetransmissionRequest(request)
      result shouldBe Left(
        "Got a malformed retransmission request from node0 at segment 0, wrong size of pbft-messages list, ignoring"
      )
    }

    "validate correctly status with well formed in-progress block" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val request = retransmissionRequest(
        segments = Seq(
          SegmentStatus.InProgress(
            ViewNumber.First,
            Seq.fill(epochLength.toInt)(BlockStatus.InProgress(false, Seq(false), Seq(false))),
          )
        )
      )
      val result = validator.validateRetransmissionRequest(request)
      result shouldBe Right(())
    }

  }

  "RetransmissionMessageValidator.validateRetransmissionResponse" should {
    "successfully validate message" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val pp = prePrepare(epochNumber = 0L, blockNumber = 0L)
      val cc = CommitCertificate(pp, Seq(commit(EpochNumber.First, 0L, pp.message.hash)))

      val result =
        validator.validateRetransmissionResponse(RetransmissionResponse.create(otherId, Seq(cc)))
      result shouldBe Right(())
    }

    "error when message has no commit certificates" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val result =
        validator.validateRetransmissionResponse(RetransmissionResponse.create(otherId, Seq.empty))
      result shouldBe Left(
        RetransmissionResponseValidationError.MalformedMessage(
          otherId,
          "no commit certificates",
        )
      )
    }

    "error when message has commit certificates with multiple epoch numbers" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val cc1 = CommitCertificate(prePrepare(epochNumber = 10L, blockNumber = 10L), Seq.empty)
      val cc2 = CommitCertificate(prePrepare(epochNumber = 11L, blockNumber = 10L), Seq.empty)

      val result =
        validator.validateRetransmissionResponse(
          RetransmissionResponse.create(otherId, Seq(cc1, cc2))
        )
      result shouldBe Left(
        RetransmissionResponseValidationError.MalformedMessage(
          otherId,
          "commit certificates from different epochs 10, 11",
        )
      )
    }

    "error when message has commit certificates for the wrong epoch" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val cc = CommitCertificate(prePrepare(epochNumber = 10L, blockNumber = 10L), Seq.empty)

      val result =
        validator.validateRetransmissionResponse(RetransmissionResponse.create(otherId, Seq(cc)))
      result shouldBe Left(
        RetransmissionResponseValidationError.WrongEpoch(
          otherId,
          EpochNumber(10L),
          EpochNumber.First,
        )
      )
    }

    "error when message has commit certificates with block number outside of the epoch" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val cc =
        CommitCertificate(prePrepare(epochNumber = 0L, blockNumber = epochLength + 2), Seq.empty)

      val result =
        validator.validateRetransmissionResponse(RetransmissionResponse.create(otherId, Seq(cc)))
      result shouldBe Left(
        RetransmissionResponseValidationError.MalformedMessage(
          otherId,
          "block number(s) outside of epoch 0: 10",
        )
      )
    }

    "error when message has more than one commit certificates for the same block number" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val cc = CommitCertificate(prePrepare(epochNumber = 0L, blockNumber = 0L), Seq.empty)

      val result =
        validator.validateRetransmissionResponse(
          RetransmissionResponse.create(otherId, Seq(cc, cc))
        )
      result shouldBe Left(
        RetransmissionResponseValidationError.MalformedMessage(
          otherId,
          "multiple commit certificates for the following block number(s): 0",
        )
      )
    }

    "error when message has invalid commit certificates" in {
      val validator = new RetransmissionMessageValidator(epoch)
      val cc = CommitCertificate(prePrepare(epochNumber = 0L, blockNumber = 0L), Seq.empty)

      val result =
        validator.validateRetransmissionResponse(RetransmissionResponse.create(otherId, Seq(cc)))
      result shouldBe Left(
        RetransmissionResponseValidationError.MalformedMessage(
          otherId,
          "invalid commit certificate: commit certificate for block 0 has the following errors: there are no commits",
        )
      )
    }
  }
}

object RetransmissionMessageValidatorTest {
  val epochLength = EpochLength(8L)
  val epochInfo =
    EpochInfo.mk(
      number = EpochNumber.First,
      startBlockNumber = BlockNumber.First,
      length = epochLength,
    )
  val myId = BftNodeId("self")
  val otherId = BftNodeId(s"node0")
  val membership = Membership.forTesting(myId)
  val epoch = Epoch(epochInfo, membership, membership)

  def retransmissionRequest(segments: Seq[SegmentStatus])(implicit
      synchronizerProtocolVersion: ProtocolVersion
  ): RetransmissionRequest =
    RetransmissionRequest.create(EpochStatus(otherId, EpochNumber.First, segments))

  def prePrepare(
      epochNumber: Long,
      blockNumber: Long,
      block: OrderingBlock = OrderingBlock(Seq.empty),
  )(implicit synchronizerProtocolVersion: ProtocolVersion) =
    PrePrepare
      .create(
        BlockMetadata.mk(epochNumber, blockNumber),
        ViewNumber(ViewNumber.First),
        block,
        CanonicalCommitSet(Set.empty),
        from = myId,
      )
      .fakeSign

  private def commit(
      epochNumber: Long,
      blockNumber: Long,
      hash: Hash,
      from: BftNodeId = myId,
  )(implicit synchronizerProtocolVersion: ProtocolVersion) =
    Commit
      .create(
        BlockMetadata.mk(epochNumber, blockNumber),
        ViewNumber.First,
        hash,
        CantonTimestamp.Epoch,
        from,
      )
      .fakeSign
}
