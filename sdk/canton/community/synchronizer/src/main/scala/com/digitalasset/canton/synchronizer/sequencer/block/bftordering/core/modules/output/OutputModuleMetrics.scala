// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
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
    metrics.output.blockDelay.update(
      Duration.between(orderedBlockBftTime.toInstant, orderingCompletionInstant)
    )(outputMc)
    metrics.global.blocksOrdered.mark(1L)
    orderedBlockData.batches.foreach { batch =>
      batch._2.requests.foreach { request =>
        request.value.orderingStartInstant.foreach { i =>
          metrics.global.requestsOrderingLatency.timer.update(
            Duration.between(i, orderingCompletionInstant)
          )(
            mc.withExtraLabels(
              metrics.global.requestsOrderingLatency.labels.ReceivingSequencer ->
                // Only used when there are requests, in which case it matches the initial ISS segment leader,
                //  which is also the receiving sequencer.
                orderedBlockData.orderedBlockForOutput.from
            )
          )
        }
      }
    }
  }
}
