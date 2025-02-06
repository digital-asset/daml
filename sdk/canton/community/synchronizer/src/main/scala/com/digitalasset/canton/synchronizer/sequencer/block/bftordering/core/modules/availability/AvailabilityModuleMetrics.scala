// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.topology.SequencerId

private[availability] object AvailabilityModuleMetrics {

  def emitInvalidMessage(metrics: BftOrderingMetrics, from: SequencerId)(implicit
      mc: MetricsContext
  ): Unit =
    metrics.security.noncompliant.behavior.mark()(
      mc.withExtraLabels(
        metrics.security.noncompliant.labels.Sequencer -> from.toProtoPrimitive,
        metrics.security.noncompliant.labels.violationType.Key ->
          metrics.security.noncompliant.labels.violationType.values.DisseminationInvalidMessage,
      )
    )

  def emitDisseminationStateStats(
      metrics: BftOrderingMetrics,
      state: DisseminationProtocolState,
  )(implicit mc: MetricsContext): Unit = {
    metrics.availability.requestedProposals.updateValue(state.toBeProvidedToConsensus.size)
    metrics.availability.requestedBatches.updateValue(
      state.toBeProvidedToConsensus.map(_.maxBatchesPerProposal.toInt).sum
    )
    metrics.availability.readyBytes.updateValue(
      state.batchesReadyForOrdering.map(_._2.stats.bytes).sum
    )
    metrics.availability.readyRequests.updateValue(
      state.batchesReadyForOrdering.map(_._2.stats.requests).sum
    )
    metrics.availability.readyBatches.updateValue(state.batchesReadyForOrdering.size)
  }
}
