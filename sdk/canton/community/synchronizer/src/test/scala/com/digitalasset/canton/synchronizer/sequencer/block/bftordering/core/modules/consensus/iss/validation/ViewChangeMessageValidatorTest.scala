// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.ViewChangeMessageValidator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  NewView,
  PrePrepare,
  Prepare,
  ViewChange,
}
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class ViewChangeMessageValidatorTest extends AnyWordSpec with BftSequencerBaseTest {
  private val myId = fakeSequencerId("self")
  private val otherId = fakeSequencerId("otherId")
  private val membership = Membership.forTesting(myId, Set(otherId))
  private val epochNumber = EpochNumber(0L)
  private val blockNumbers = NonEmpty(Seq, 1L, 3L, 5L).map(BlockNumber(_))
  private val wrongHash = Hash.digest(
    HashPurpose.BftOrderingPbftBlock,
    ByteString.copyFromUtf8("bad data"),
    HashAlgorithm.Sha256,
  )

  private val view0 = ViewNumber.First
  private val view1 = ViewNumber(view0 + 1)
  private val view2 = ViewNumber(view1 + 1)

  private def prePrepare(
      epochNumber: Long,
      blockNumber: Long,
      viewNumber: Long,
      block: OrderingBlock = OrderingBlock(Seq.empty),
  ) = PrePrepare
    .create(
      BlockMetadata.mk(epochNumber, blockNumber),
      ViewNumber(viewNumber),
      CantonTimestamp.Epoch,
      block,
      CanonicalCommitSet(Set.empty),
      from = myId,
    )
    .fakeSign

  private def prepare(
      epochNumber: Long,
      blockNumber: Long,
      hash: Hash,
      viewNumber: Long = view0,
      from: SequencerId = myId,
  ) =
    Prepare
      .create(
        BlockMetadata.mk(epochNumber, blockNumber),
        ViewNumber(viewNumber),
        hash,
        CantonTimestamp.Epoch,
        from,
      )
      .fakeSign

  private def commit(
      epochNumber: Long,
      blockNumber: Long,
      hash: Hash,
      viewNumber: Long = ViewNumber.First,
      from: SequencerId = myId,
  ) =
    Commit
      .create(
        BlockMetadata.mk(epochNumber, blockNumber),
        ViewNumber(viewNumber),
        hash,
        CantonTimestamp.Epoch,
        from,
      )
      .fakeSign

  private def viewChangeMsg(
      viewNumber: ViewNumber,
      consensusCerts: Seq[ConsensusCertificate],
      epochNumber: Long = 0L,
      segment: Long = 0L,
      from: SequencerId = myId,
  ) =
    ViewChange.create(
      BlockMetadata(EpochNumber(epochNumber), BlockNumber(segment)),
      0,
      viewNumber,
      CantonTimestamp.Epoch,
      consensusCerts,
      from,
    )

  private def newViewMessage(
      viewNumber: ViewNumber,
      viewChanges: Seq[SignedMessage[ViewChange]],
      prePrepares: Seq[SignedMessage[PrePrepare]],
      from: SequencerId = myId,
  ): NewView = NewView.create(
    BlockMetadata(EpochNumber(0), BlockNumber(0)),
    0,
    viewNumber,
    CantonTimestamp.Epoch,
    viewChanges,
    prePrepares,
    from,
  )

  "ViewChangeMessageValidator.validateViewChangeMessage" should {
    "successfully validate view-change message with no certificates" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val result = validator.validateViewChangeMessage(viewChangeMsg(view1, Seq.empty))

      result shouldBe Right(())
    }

    "error when message has prepare certificates for the wrong epoch" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)
      val pc1 = PrepareCertificate(prePrepare(epochNumber + 1, 1L, view0), Seq.empty)
      val pc2 = PrepareCertificate(prePrepare(epochNumber + 2, 1L, view0), Seq.empty)

      val result = validator.validateViewChangeMessage(viewChangeMsg(view1, Seq(pc1, pc2)))
      result shouldBe Left("there are consensus certs for the wrong epoch (1, 2)")
    }

    "error when message has prepare certificates for the wrong segment" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)
      val pc1 = PrepareCertificate(prePrepare(epochNumber, 0L, view0), Seq.empty)
      val pc2 = PrepareCertificate(prePrepare(epochNumber, 2L, view0), Seq.empty)

      val result = validator.validateViewChangeMessage(viewChangeMsg(view1, Seq(pc1, pc2)))
      result shouldBe Left("there are consensus certs for blocks from the wrong segment (0, 2)")
    }

    "error when message has more than one prepare certificates for the same block" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)
      val pc1 = PrepareCertificate(prePrepare(epochNumber, 1L, view0), Seq.empty)
      val pc2 = PrepareCertificate(prePrepare(epochNumber, 1L, view0), Seq.empty)
      val pc3 = PrepareCertificate(prePrepare(epochNumber, 3L, view0), Seq.empty)
      val pc4 = PrepareCertificate(prePrepare(epochNumber, 3L, view0), Seq.empty)

      val result =
        validator.validateViewChangeMessage(viewChangeMsg(view1, Seq(pc1, pc2, pc3, pc4)))

      result shouldBe Left(
        "there are more than one consensus certificates for the following blocks (1, 3)"
      )
    }

    "error when message has certificates with pre-prepares from the current view number or later" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)
      val pc1 = PrepareCertificate(prePrepare(epochNumber, 1L, view2), Seq.empty)
      val pc2 = PrepareCertificate(prePrepare(epochNumber, 3L, view1), Seq.empty)

      val result = validator.validateViewChangeMessage(viewChangeMsg(view1, Seq(pc1, pc2)))

      result shouldBe Left(
        "there are consensus certificate pre-prepares with view numbers (2, 1) higher than or at current view number 1"
      )
    }

    "successfully validate message with good certificates" in {
      val validator =
        new ViewChangeMessageValidator(Membership.forTesting(myId), blockNumbers)

      val pp1 = prePrepare(epochNumber, 1L, view0)
      val pp3 = prePrepare(epochNumber, 3L, view0)

      val pc = PrepareCertificate(pp1, Seq(prepare(epochNumber, 1L, pp1.message.hash)))
      val cc = CommitCertificate(pp3, Seq(commit(epochNumber, 3L, pp3.message.hash)))

      val result =
        validator.validateViewChangeMessage(viewChangeMsg(view1, Seq[ConsensusCertificate](pc, cc)))

      result shouldBe Right(())
    }

    "error when certificates have no messages in them" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)
      val pc = PrepareCertificate(prePrepare(epochNumber, 1L, view0), Seq.empty)
      val cc = CommitCertificate(prePrepare(epochNumber, 3L, view0), Seq.empty)

      val result =
        validator.validateViewChangeMessage(viewChangeMsg(view1, Seq[ConsensusCertificate](pc, cc)))

      result shouldBe Left(
        "prepare certificate for block 1 has the following errors: there are no prepares, commit certificate for block 3 has the following errors: there are no commits"
      )
    }

    "error when certificates have messages with multiples view numbers of not behind current view number" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val pp1 = prePrepare(epochNumber, 1L, view0)
      val pp3 = prePrepare(epochNumber, 3L, view0)

      val pc = PrepareCertificate(
        pp1,
        Seq(
          prepare(epochNumber, 1L, pp1.message.hash, view2),
          prepare(epochNumber, 1L, pp1.message.hash, view2, from = otherId),
        ),
      )
      val cc = CommitCertificate(
        pp3,
        Seq(
          commit(epochNumber, 3L, pp3.message.hash, view0),
          commit(epochNumber, 3L, pp3.message.hash, view1, from = otherId),
        ),
      )

      val result =
        validator.validateViewChangeMessage(viewChangeMsg(view2, Seq[ConsensusCertificate](pc, cc)))

      result shouldBe Left(
        "prepare certificate for block 1 has the following errors: prepares have view number 2 but it should be less than current view number 2, commit certificate for block 3 has the following errors: all commits should be of the same view number, but they are distributed across multiple view numbers (0, 1)"
      )
    }

    "error when certificates have multiple messages from the same sender" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val pp1 = prePrepare(epochNumber, 1L, view0)

      val pc = PrepareCertificate(
        pp1,
        Seq(
          prepare(epochNumber, 1L, pp1.message.hash),
          prepare(epochNumber, 1L, pp1.message.hash),
          prepare(epochNumber, 1L, pp1.message.hash, from = otherId),
        ),
      )

      val result = validator.validateViewChangeMessage(viewChangeMsg(view2, Seq(pc)))

      result shouldBe Left(
        s"prepare certificate for block 1 has the following errors: there are more than one prepares (2) from the same sender $myId"
      )
    }

    "error when certificates have messages from blocks different than the one from the pre-prepare" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val pp1 = prePrepare(epochNumber, 1L, view0)
      val pp3 = prePrepare(epochNumber, 3L, view0)

      val pc = PrepareCertificate(
        pp1,
        Seq(
          prepare(epochNumber, 3L, pp1.message.hash, view1),
          prepare(epochNumber, 3L, pp1.message.hash, view1, from = otherId),
        ),
      )
      val cc = CommitCertificate(
        pp3,
        Seq(
          commit(epochNumber, 1L, pp3.message.hash, view1),
          commit(epochNumber, 2L, pp3.message.hash, view1, from = otherId),
        ),
      )

      val result =
        validator.validateViewChangeMessage(viewChangeMsg(view2, Seq[ConsensusCertificate](pc, cc)))

      result shouldBe Left(
        "prepare certificate for block 1 has the following errors: there are prepares for the wrong block number (3), commit certificate for block 3 has the following errors: there are commits for the wrong block number (1, 2)"
      )
    }

    "error when certificates have fewer messages than needed to reach strong quorum" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val pp3 = prePrepare(epochNumber, 3L, view0)
      val cc = CommitCertificate(
        pp3,
        Seq(
          commit(epochNumber, 3L, pp3.message.hash, view1)
        ),
      )

      val result =
        validator.validateViewChangeMessage(viewChangeMsg(view2, Seq(cc)))

      result shouldBe Left(
        "commit certificate for block 3 has the following errors: expected at least 2 commits, but only got 1"
      )
    }

    "error when certificates have messages with wrong hash" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val pp1 = prePrepare(epochNumber, 1L, view0)
      val pp3 = prePrepare(epochNumber, 3L, view0)

      val pc = PrepareCertificate(
        pp1,
        Seq(
          prepare(epochNumber, 1L, wrongHash, view1),
          prepare(epochNumber, 1L, wrongHash, view1, from = otherId),
        ),
      )
      val cc = CommitCertificate(
        pp3,
        Seq(
          commit(epochNumber, 3L, wrongHash, view1),
          commit(epochNumber, 3L, wrongHash, view1, from = otherId),
        ),
      )

      val result =
        validator.validateViewChangeMessage(viewChangeMsg(view2, Seq[ConsensusCertificate](pc, cc)))

      result shouldBe Left(
        s"prepare certificate for block 1 has the following errors: prepare from $myId has non-matching hash, commit certificate for block 3 has the following errors: commit from $myId has non-matching hash"
      )
    }
  }

  "ViewChangeMessageValidator.validateNewViewMessage" should {
    "successfully validate new-view message with empty view-change messages" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq.empty).fakeSign,
          viewChangeMsg(view1, Seq.empty, from = otherId).fakeSign,
        ),
        Seq(
          prePrepare(epochNumber, 1L, view1),
          prePrepare(epochNumber, 3L, view1),
          prePrepare(epochNumber, 5L, view1),
        ),
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Right(())
    }

    "error when there are view changes are for wrong epoch" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq.empty, epochNumber = 3).fakeSign,
          viewChangeMsg(view1, Seq.empty, epochNumber = 2).fakeSign,
        ),
        Seq.empty,
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Left("there are view change messages for the wrong epoch (3, 2 instead of 0)")
    }

    "error when there are view changes are for wrong segment" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq.empty, segment = 3).fakeSign,
          viewChangeMsg(view1, Seq.empty, segment = 2).fakeSign,
        ),
        Seq.empty,
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Left(
        "there are view change messages for the wrong segment identifier (3, 2 instead of 0)"
      )
    }

    "error when there are view changes are for wrong view" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view0, Seq.empty).fakeSign,
          viewChangeMsg(view2, Seq.empty).fakeSign,
        ),
        Seq.empty,
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Left(
        "there are view change messages for the wrong view (0, 2 instead of 1)"
      )
    }

    "error when there are multiple view changes from the same sender" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq.empty).fakeSign,
          viewChangeMsg(view1, Seq.empty).fakeSign,
        ),
        Seq.empty,
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Left(
        "there are more than one view change messages from the same sender for the following nodes: SEQ::ns::fake_self"
      )
    }

    "error when there is not exactly a quorum of view change messages" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq.empty).fakeSign
        ),
        Seq.empty,
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Left(
        "expected 2 view-change messages, but got 1"
      )
    }

    "error when there is an invalid view change" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val invalidViewChangeMessage = viewChangeMsg(
        view1,
        // prepare certificate with wrong epoch
        Seq(PrepareCertificate(prePrepare(epochNumber + 1, 1L, view0), Seq.empty)),
        from = otherId,
      ).fakeSign

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq.empty).fakeSign,
          invalidViewChangeMessage,
        ),
        Seq.empty,
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Left(
        "view change message from SEQ::ns::fake_otherId is invalid: there are consensus certs for the wrong epoch (1)"
      )
    }

    "error when pre-prepares are not exactly for the blocks in the segment" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq.empty).fakeSign,
          viewChangeMsg(view1, Seq.empty, from = otherId).fakeSign,
        ),
        Seq(
          prePrepare(epochNumber, 1L, view1),
          prePrepare(epochNumber, 2L, view1),
          prePrepare(epochNumber, 5L, view1),
        ),
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Left(
        "expected pre-prepares to be for blocks (in-order) 1, 3, 5 but instead they were for 1, 2, 5"
      )
    }
    "error when there are pre-prepares for the wrong epoch" in {
      val validator = new ViewChangeMessageValidator(membership, blockNumbers)

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq.empty).fakeSign,
          viewChangeMsg(view1, Seq.empty, from = otherId).fakeSign,
        ),
        Seq(
          prePrepare(epochNumber, 1L, view1),
          prePrepare(epochNumber + 1, 3L, view1),
          prePrepare(epochNumber + 2, 5L, view1),
        ),
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Left(
        "there are pre-prepares for the wrong epoch (1, 2 instead of 0)"
      )
    }

    "successfully validate new-view with correctly picked pre-prepares from certificates" in {
      val validator =
        new ViewChangeMessageValidator(Membership.forTesting(myId), blockNumbers)

      val pp1 = prePrepare(epochNumber, 1L, view0)
      val pp3 = prePrepare(epochNumber, 3L, view0)

      val pc = PrepareCertificate(pp1, Seq(prepare(epochNumber, 1L, pp1.message.hash)))
      val cc = CommitCertificate(pp3, Seq(commit(epochNumber, 3L, pp3.message.hash)))

      val newView = newViewMessage(
        view1,
        Seq(
          viewChangeMsg(view1, Seq[ConsensusCertificate](pc, cc)).fakeSign
        ),
        Seq(pp1, pp3, prePrepare(epochNumber, 5L, view1)),
      )

      val result = validator.validateNewViewMessage(newView)

      result shouldBe Right(())
    }
  }

  "error when pre-prepares don't match the ones from the certificates" in {
    val validator = new ViewChangeMessageValidator(Membership.forTesting(myId), blockNumbers)

    val pp1 = prePrepare(epochNumber, 1L, view0)
    val pp3 = prePrepare(epochNumber, 3L, view0)

    val pc = PrepareCertificate(pp1, Seq(prepare(epochNumber, 1L, pp1.message.hash)))
    val cc = CommitCertificate(pp3, Seq(commit(epochNumber, 3L, pp3.message.hash)))

    val newView = newViewMessage(
      view1,
      Seq(
        viewChangeMsg(view1, Seq[ConsensusCertificate](pc, cc)).fakeSign
      ),
      Seq(
        prePrepare(epochNumber, 1L, view1),
        prePrepare(epochNumber, 3L, view1),
        prePrepare(epochNumber, 5L, view1),
      ),
    )

    val result = validator.validateNewViewMessage(newView)

    result shouldBe Left(
      "pre-prepare for block 1 does not match the one expected from consensus certificate, pre-prepare for block 3 does not match the one expected from consensus certificate"
    )
  }

  "error when bottom pre-prepares don't match expectations of what they should look like" in {
    val validator = new ViewChangeMessageValidator(Membership.forTesting(myId), blockNumbers)

    val newView = newViewMessage(
      view1,
      Seq(
        viewChangeMsg(view1, Seq.empty).fakeSign
      ),
      Seq(
        prePrepare(epochNumber, 1L, view1 + 1),
        prePrepare(
          epochNumber,
          3L,
          view1,
          block = OrderingBlock(
            Seq(
              ProofOfAvailability(
                BatchId.createForTesting("hash"),
                Seq.empty,
                CantonTimestamp.MaxValue,
              )
            )
          ),
        ),
        prePrepare(epochNumber, 5L, view1),
      ),
    )

    val result = validator.validateNewViewMessage(newView)

    result shouldBe Left(
      "pre-prepare for bottom block 1 should be for view 1 but it is for 2, pre-prepare for block 3 should be for bottom block, but it contains proofs of availability"
    )
  }
}
