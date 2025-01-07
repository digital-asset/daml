// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.CompleteBlockData
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}

object Output {

  sealed trait Message[+E] extends Product

  final case object Start extends Message[Nothing]

  // From local consensus
  final case class BlockOrdered(orderedBlockForOutput: OrderedBlockForOutput)
      extends Message[Nothing]

  // From local availability storage
  final case class BlockDataFetched(data: CompleteBlockData) extends Message[Nothing]

  final case class BlockDataStored(
      orderedBlockData: CompleteBlockData,
      orderedBlockNumber: BlockNumber,
      orderedBlockBftTime: CantonTimestamp,
      epochCouldAlterSequencingTopology: Boolean,
  ) extends Message[Nothing]

  final case class TopologyFetched[E <: Env[E]](
      lastCompletedBlockNumber: BlockNumber,
      lastCompletedBlockMode: OrderedBlockForOutput.Mode,
      newEpochNumber: EpochNumber,
      orderingTopology: OrderingTopology,
      cryptoProvider: CryptoProvider[E],
  ) extends Message[E]

  final case class LastBlockUpdated[E <: Env[E]](
      lastCompletedBlockNumber: BlockNumber,
      lastCompletedBlockMode: OrderedBlockForOutput.Mode,
      newEpochNumber: EpochNumber,
      orderingTopology: OrderingTopology,
      cryptoProvider: CryptoProvider[E],
  ) extends Message[E]

  final case class AsyncException(error: Throwable) extends Message[Nothing]

  final case object NoTopologyAvailable extends Message[Nothing]

  sealed trait SequencerSnapshotMessage extends Message[Nothing]
  object SequencerSnapshotMessage {
    final case class GetAdditionalInfo(
        timestamp: CantonTimestamp,
        from: ModuleRef[SequencerNode.SnapshotMessage],
    ) extends SequencerSnapshotMessage

    final case class AdditionalInfo(
        requester: ModuleRef[SequencerNode.SnapshotMessage],
        info: SequencerSnapshotAdditionalInfo,
    ) extends SequencerSnapshotMessage

    final case class AdditionalInfoRetrievalError(
        requester: ModuleRef[SequencerNode.SnapshotMessage],
        errorMessage: String,
    ) extends SequencerSnapshotMessage
  }
}

trait Output[E <: Env[E]] extends Module[E, Output.Message[E]] with FlagCloseable {

  def availability: ModuleRef[Availability.Message[E]]
  def consensus: ModuleRef[Consensus.Message[E]]
}
