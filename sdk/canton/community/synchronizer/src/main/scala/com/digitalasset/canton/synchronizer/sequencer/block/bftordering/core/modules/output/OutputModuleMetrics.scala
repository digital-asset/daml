// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.CompleteBlockData
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput

import java.time.{Duration, Instant}

private[output] object OutputModuleMetrics {

  def emitRequestsOrderingStats(
      metrics: BftOrderingMetrics,
      orderedBlockData: CompleteBlockData,
      orderedBlockBftTime: CantonTimestamp,
  )(implicit mc: MetricsContext): Unit = {
    val orderingCompletionInstant = Instant.now
    val requests = orderedBlockData.batches.flatMap(_._2.requests.map(_.value))
    val bytesOrdered = requests.map(_.payload.size().toLong).sum
    val requestsOrdered = requests.length.toLong
    val batchesOrdered = orderedBlockData.batches.length.toLong
    val blockMode =
      orderedBlockData.orderedBlockForOutput.mode match {
        case OrderedBlockForOutput.Mode.FromConsensus =>
          metrics.output.labels.mode.values.Consensus
        case OrderedBlockForOutput.Mode.FromStateTransfer =>
          metrics.output.labels.mode.values.StateTransfer
      }
    val outputMc = mc.withExtraLabels(metrics.output.labels.mode.Key -> blockMode)

    metrics.output.blockSizeBytes.update(bytesOrdered)(outputMc)
    metrics.output.blockSizeRequests.update(requestsOrdered)(outputMc)
    metrics.output.blockSizeBatches.update(batchesOrdered)(outputMc)
    updateTimer(
      metrics.output.blockDelay,
      Duration.between(orderedBlockBftTime.toInstant, orderingCompletionInstant),
    )(outputMc)
    metrics.global.blocksOrdered.mark(1L)(
      mc.withExtraLabels(
        metrics.global.labels.IsBlockEmpty -> orderedBlockData.batches.isEmpty.toString
      )
    )
    metrics.global.batchesOrdered.mark(orderedBlockData.batches.size.toLong)
    metrics.global.requestsOrdered.mark(orderedBlockData.requestsView.size.toLong)
    orderedBlockData.batches.foreach { batch =>
      batch._2.requests.foreach { request =>
        request.value.orderingStartInstant.foreach { i =>
          updateTimer(
            metrics.global.requestsOrderingLatency.timer,
            Duration.between(i, orderingCompletionInstant),
          )(
            mc.withExtraLabels(
              metrics.global.requestsOrderingLatency.labels.ReceivingSequencer ->
                // Only used when there are requests, in which case it matches the initial ISS segment leader,
                //  which is also the receiving sequencer.
                orderedBlockData.orderedBlockForOutput.originalLeader
            )
          )
        }
      }
    }
  }
}
