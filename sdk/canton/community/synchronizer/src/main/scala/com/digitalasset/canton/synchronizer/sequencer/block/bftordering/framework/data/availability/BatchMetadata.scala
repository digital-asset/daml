// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatchStats
import com.digitalasset.canton.tracing.Traced

import java.time.Instant

final case class InProgressBatchMetadata(
    batchId: Traced[BatchId],
    epochNumber: EpochNumber,
    stats: OrderingRequestBatchStats,
    // The following fields are only used for metrics
    availabilityEnterInstant: Option[Instant] = None,
    regressionsToSigning: Int = 0,
    disseminationRegressions: Int = 0,
) {

  def complete(acks: Seq[AvailabilityAck]): DisseminatedBatchMetadata =
    DisseminatedBatchMetadata(
      batchId.map(ProofOfAvailability(_, acks, epochNumber)),
      epochNumber,
      stats,
      availabilityEnterInstant = availabilityEnterInstant,
      readyForOrderingInstant = Some(Instant.now),
      regressionsToSigning = regressionsToSigning,
      disseminationRegressions = disseminationRegressions,
    )
}

final case class DisseminatedBatchMetadata(
    proofOfAvailability: Traced[ProofOfAvailability],
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
      proofOfAvailability.map(_.batchId),
      epochNumber,
      stats,
      availabilityEnterInstant,
      regressionsToSigning + toSigning.compareTo(false),
      disseminationRegressions + (!toSigning).compareTo(false),
    )
}
