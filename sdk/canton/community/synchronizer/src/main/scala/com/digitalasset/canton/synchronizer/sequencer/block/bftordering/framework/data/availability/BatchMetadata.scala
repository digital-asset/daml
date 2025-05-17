// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatchStats

import java.time.Instant

final case class InProgressBatchMetadata(
    batchId: BatchId,
    epochNumber: EpochNumber,
    stats: OrderingRequestBatchStats,
    // The following fields are only used for metrics
    availabilityEnterInstant: Option[Instant] = None,
    regressionsToSigning: Int = 0,
    disseminationRegressions: Int = 0,
) {

  def complete(acks: Seq[AvailabilityAck]): DisseminatedBatchMetadata =
    DisseminatedBatchMetadata(
      ProofOfAvailability(batchId, acks, epochNumber),
      epochNumber,
      stats,
      availabilityEnterInstant = availabilityEnterInstant,
      readyForOrderingInstant = Some(Instant.now),
      regressionsToSigning = regressionsToSigning,
      disseminationRegressions = disseminationRegressions,
    )
}

final case class DisseminatedBatchMetadata(
    proofOfAvailability: ProofOfAvailability,
    epochNumber: EpochNumber,
    stats: OrderingRequestBatchStats,
    // The following fields are only used for metrics
    availabilityEnterInstant: Option[Instant] = None,
    readyForOrderingInstant: Option[Instant] = None,
    regressionsToSigning: Int = 0,
    disseminationRegressions: Int = 0,
) {
  def regress(toSigning: Boolean): InProgressBatchMetadata =
    InProgressBatchMetadata(
      proofOfAvailability.batchId,
      epochNumber,
      stats,
      availabilityEnterInstant,
      regressionsToSigning + toSigning.compareTo(false),
      disseminationRegressions + (!toSigning).compareTo(false),
    )
}
