// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}

final case class BftPruningStatus(
    latestBlockEpochNumber: EpochNumber,
    latestBlockNumber: BlockNumber,
    latestBlockTimestamp: CantonTimestamp,
    lowerBoundEpochNumber: EpochNumber,
    lowerBoundBlockNumber: BlockNumber,
) {

  def toProto = v30.BftPruningStatusResponse(
    latestBlockEpoch = latestBlockEpochNumber,
    latestBlock = latestBlockNumber,
    latestBlockTimestamp = Some(latestBlockTimestamp.toProtoTimestamp),
    lowerBoundEpoch = lowerBoundEpochNumber,
    lowerBoundBlock = lowerBoundBlockNumber,
  )

}
