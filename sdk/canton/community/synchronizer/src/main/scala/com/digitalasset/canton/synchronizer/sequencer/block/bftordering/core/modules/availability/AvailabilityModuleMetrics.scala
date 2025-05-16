// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

private[availability] object AvailabilityModuleMetrics {

  def emitInvalidMessage(metrics: BftOrderingMetrics, from: BftNodeId)(implicit
      mc: MetricsContext
  ): Unit = {
    import metrics.security.noncompliant.*
    behavior.mark()(
      mc.withExtraLabels(
        labels.Sequencer -> from,
        labels.violationType.Key ->
          labels.violationType.values.DisseminationInvalidMessage,
      )
    )
  }

  def emitDisseminationStateStats(
      metrics: BftOrderingMetrics,
      state: DisseminationProtocolState,
  )(implicit mc: MetricsContext): Unit = {
    import metrics.availability.*

    requested.proposals.updateValue(state.toBeProvidedToConsensus.size)
    requested.batches.updateValue(
      state.toBeProvidedToConsensus.map(_.maxBatchesPerProposal.toInt).sum
    )

    val readyForConsensusMc = mc.withExtraLabels(dissemination.labels.ReadyForConsensus -> "true")
    locally {
      implicit val mc: MetricsContext = readyForConsensusMc
      dissemination.bytes.updateValue(state.batchesReadyForOrdering.values.map(_.stats.bytes).sum)
      dissemination.requests.updateValue(
        state.batchesReadyForOrdering.values.map(_.stats.requests).sum
      )
      dissemination.batches.updateValue(state.batchesReadyForOrdering.size)
    }
    val inProgressMc = mc.withExtraLabels(dissemination.labels.ReadyForConsensus -> "false")
    locally {
      implicit val mc: MetricsContext = inProgressMc
      dissemination.bytes.updateValue(
        state.disseminationProgress.values.map(_.batchMetadata.stats.bytes).sum
      )
      dissemination.requests.updateValue(
        state.disseminationProgress.values.map(_.batchMetadata.stats.requests).sum
      )
      dissemination.batches.updateValue(state.disseminationProgress.size)
    }

    val regressionsToSigning =
      state.disseminationProgress.values.map(_.batchMetadata.regressionsToSigning).sum +
        state.batchesReadyForOrdering.values.map(_.regressionsToSigning).sum
    val regressedDisseminations =
      state.disseminationProgress.values.map(_.batchMetadata.disseminationRegressions).sum +
        state.batchesReadyForOrdering.values.map(_.disseminationRegressions).sum
    regression.batch.inc(regressionsToSigning.toLong)(
      mc.withExtraLabels(
        regression.labels.stage.Key -> regression.labels.stage.values.Signing.toString
      )
    )
    regression.batch.inc(regressedDisseminations.toLong)(
      mc.withExtraLabels(
        regression.labels.stage.Key -> regression.labels.stage.values.Dissemination.toString
      )
    )
    // Reset regressions counts to avoid double counting
    state.disseminationProgress = state.disseminationProgress.map { case (key, value) =>
      key -> value.copy(
        batchMetadata = value.batchMetadata.copy(
          regressionsToSigning = 0,
          disseminationRegressions = 0,
        )
      )
    }
    state.batchesReadyForOrdering = state.batchesReadyForOrdering.map { case (key, value) =>
      key -> value.copy(
        regressionsToSigning = 0,
        disseminationRegressions = 0,
      )
    }
  }
}
