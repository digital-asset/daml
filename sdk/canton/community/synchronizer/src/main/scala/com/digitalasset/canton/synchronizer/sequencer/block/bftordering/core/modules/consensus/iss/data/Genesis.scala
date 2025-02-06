// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo

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

  // Note that the genesis epoch does not contain commits, which results in using empty canonical commit sets for
  //  the first blocks proposed by each leader. These blocks are required to be empty so that actual transactions
  //  are not assigned inaccurate timestamps.
  val GenesisEpoch: Epoch = Epoch(GenesisEpochInfo, lastBlockCommits = Seq.empty)
}
