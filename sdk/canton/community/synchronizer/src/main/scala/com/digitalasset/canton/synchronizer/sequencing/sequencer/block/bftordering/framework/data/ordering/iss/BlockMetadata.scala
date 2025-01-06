// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss

import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.BlockMetadata as ProtoBlockMetadata
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.google.common.annotations.VisibleForTesting

final case class BlockMetadata(
    epochNumber: EpochNumber,
    blockNumber: BlockNumber,
) {
  def toProto: ProtoBlockMetadata =
    ProtoBlockMetadata.of(
      epochNumber,
      blockNumber,
    )
}

object BlockMetadata {

  /** A convenience constructor for tests */
  @VisibleForTesting
  def mk(epochNumber: Long, blockNumber: Long): BlockMetadata =
    new BlockMetadata(EpochNumber(epochNumber), BlockNumber(blockNumber))

  def fromProto(blockMetadata: ProtoBlockMetadata): ParsingResult[BlockMetadata] =
    Right(
      BlockMetadata(
        EpochNumber(blockMetadata.epochNumber),
        BlockNumber(blockMetadata.blockNumber),
      )
    )
}
