// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisTopologySnapshotEffectiveTime
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.google.common.annotations.VisibleForTesting

final case class EpochInfo(
    number: EpochNumber,
    startBlockNumber: BlockNumber,
    length: EpochLength,
    topologySnapshotEffectiveTime: EffectiveTime,
) {

  def relativeBlockIndex(blockNumber: BlockNumber): Int =
    (blockNumber - startBlockNumber).toInt

  def next(length: EpochLength): EpochInfo =
    copy(
      EpochNumber(number + 1),
      startBlockNumber = startOfNextEpochBlockNumber,
      length = length,
    )

  def startOfNextEpochBlockNumber: BlockNumber =
    BlockNumber(startBlockNumber + length)

  def lastBlockNumber: BlockNumber = BlockNumber(startBlockNumber + length - 1)
}

object EpochInfo {

  /** A convenience constructor for tests, also so that we don't have to provide timestamps. */
  @VisibleForTesting
  def mk(
      number: Long,
      startBlockNumber: Long,
      length: Long,
      topologySnapshotEffectiveTime: EffectiveTime = GenesisTopologySnapshotEffectiveTime,
  ): EpochInfo =
    apply(
      EpochNumber(number),
      BlockNumber(startBlockNumber),
      EpochLength(length),
      topologySnapshotEffectiveTime,
    )
}
