// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatchStats

final case class InProgressBatchMetadata(
    batchId: BatchId,
    epochNumber: EpochNumber,
    stats: OrderingRequestBatchStats,
) {

  def complete(acks: Seq[AvailabilityAck]): DisseminatedBatchMetadata =
    DisseminatedBatchMetadata(
      ProofOfAvailability(batchId, acks, epochNumber),
      epochNumber,
      stats,
    )
}

final case class DisseminatedBatchMetadata(
    proofOfAvailability: ProofOfAvailability,
    epochNumber: EpochNumber,
    stats: OrderingRequestBatchStats,
) {
  def regress(): InProgressBatchMetadata =
    InProgressBatchMetadata(
      proofOfAvailability.batchId,
      epochNumber,
      stats,
    )
}
