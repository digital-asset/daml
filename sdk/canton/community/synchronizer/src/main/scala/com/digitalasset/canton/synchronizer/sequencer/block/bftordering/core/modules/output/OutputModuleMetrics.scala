// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.CompleteBlockData

import java.time.{Duration, Instant}

private[output] object OutputModuleMetrics {

  def emitRequestsOrderingStats(
      metrics: BftOrderingMetrics,
      orderedBlockData: CompleteBlockData,
  )(implicit mc: MetricsContext): Unit = {
    val orderingCompletionInstant = Instant.now
    val requests = orderedBlockData.batches.flatMap(_._2.requests.map(_.value))
    val bytesOrdered = requests.map(_.payload.size().toLong).sum
    val requestsOrdered = requests.length.toLong
    val batchesOrdered = orderedBlockData.batches.length.toLong
    metrics.output.blockSizeBytes.update(bytesOrdered)
    metrics.output.blockSizeRequests.update(requestsOrdered)
    metrics.output.blockSizeBatches.update(batchesOrdered)
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
