// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatchStats

final case class InProgressBatchMetadata(
    batchId: BatchId,
    stats: OrderingRequestBatchStats,
    expirationTime: CantonTimestamp,
) {

  def complete(acks: Seq[AvailabilityAck]): DisseminatedBatchMetadata =
    DisseminatedBatchMetadata(ProofOfAvailability(batchId, acks, expirationTime), stats)
}

final case class DisseminatedBatchMetadata(
    proofOfAvailability: ProofOfAvailability,
    stats: OrderingRequestBatchStats,
)
