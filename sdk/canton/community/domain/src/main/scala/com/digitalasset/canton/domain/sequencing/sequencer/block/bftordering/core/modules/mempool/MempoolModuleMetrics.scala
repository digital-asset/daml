// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.mempool

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.topology.Member

private[mempool] object MempoolModuleMetrics {

  def emitRequestStats(metrics: BftOrderingMetrics)(
      orderingRequest: OrderingRequest,
      sender: Option[Member],
      outcome: metrics.ingress.labels.outcome.values.OutcomeValue,
  )(implicit mc: MetricsContext): Unit = {
    val mc1 =
      mc.withExtraLabels(
        metrics.ingress.labels.Tag -> orderingRequest.tag,
        metrics.ingress.labels.outcome.Key -> outcome,
      )
    val mc2 = sender
      .map(sender => mc1.withExtraLabels(metrics.ingress.labels.Sender -> sender.toProtoPrimitive))
      .getOrElse(mc1)
    locally {
      implicit val mc: MetricsContext = mc2
      val payloadSize = orderingRequest.payload.size()
      metrics.ingress.requestsReceived.mark(1L)
      metrics.ingress.bytesReceived.mark(payloadSize.toLong)
      metrics.ingress.requestsSize.update(payloadSize)
    }
  }

  def emitStateStats(metrics: BftOrderingMetrics, mempoolState: MempoolState)(implicit
      mc: MetricsContext
  ): Unit = {
    metrics.ingress.requestsQueued.updateValue(mempoolState.receivedOrderRequests.size)
    metrics.ingress.bytesQueued.updateValue(
      mempoolState.receivedOrderRequests.map(_.tx.value.payload.size()).sum
    )
    metrics.mempool.requestedBatches.updateValue(mempoolState.toBeProvidedToAvailability)
  }
}
