// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.google.protobuf.ByteString

object StateTransferTestHelpers {

  val myId: BftNodeId = BftNodeId("self")
  val otherId: BftNodeId = BftNodeId("other")

  val aBlockMetadata: BlockMetadata = BlockMetadata.mk(EpochNumber.First, BlockNumber.First)

  def aCommitCert(blockMetadata: BlockMetadata = aBlockMetadata): CommitCertificate =
    CommitCertificate(aPrePrepare(blockMetadata), Seq(aCommit(blockMetadata)))

  def aPrePrepare(blockMetadata: BlockMetadata): SignedMessage[PrePrepare] =
    PrePrepare
      .create(
        blockMetadata = blockMetadata,
        viewNumber = ViewNumber.First,
        block = OrderingBlock(Seq.empty),
        canonicalCommitSet = CanonicalCommitSet.empty,
        from = otherId,
      )
      .fakeSign

  def aCommit(blockMetadata: BlockMetadata = aBlockMetadata): SignedMessage[Commit] =
    Commit
      .create(
        blockMetadata,
        ViewNumber.First,
        Hash
          .digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
        CantonTimestamp.Epoch,
        from = otherId,
      )
      .fakeSign
}
