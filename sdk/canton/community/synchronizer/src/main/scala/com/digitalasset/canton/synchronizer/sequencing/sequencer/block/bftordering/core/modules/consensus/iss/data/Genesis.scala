// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString

import EpochStore.Epoch

object Genesis {

  private val GenesisStartBlockNumber = BlockNumber.First
  private val GenesisEpochLength = EpochLength(0)

  val GenesisEpochNumber: EpochNumber = EpochNumber(-1L)
  val GenesisTopologyActivationTime: TopologyActivationTime =
    OrderingTopologyProvider.InitialOrderingTopologyActivationTime

  val GenesisEpochInfo: EpochInfo =
    EpochInfo(
      GenesisEpochNumber,
      GenesisStartBlockNumber,
      GenesisEpochLength,
      GenesisTopologyActivationTime,
    )

  val GenesisEpoch: Epoch =
    Epoch(
      GenesisEpochInfo,
      lastBlockCommitMessages = Seq.empty,
    )

  def genesisCanonicalCommitSet(
      self: SequencerId,
      timestamp: CantonTimestamp,
  ): Seq[SignedMessage[Commit]] = Seq(
    SignedMessage(
      ConsensusSegment.ConsensusMessage.Commit.create(
        BlockMetadata(
          GenesisEpochNumber,
          BlockNumber(GenesisStartBlockNumber - 1),
        ),
        ViewNumber.First,
        Hash.digest(
          HashPurpose.BftOrderingPbftBlock,
          ByteString.EMPTY,
          HashAlgorithm.Sha256,
        ),
        timestamp,
        self,
      ),
      Signature.noSignature, // TODO(#22184) sign this commit to make it valid
    )
  )
}
