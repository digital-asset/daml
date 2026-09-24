// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  OrderingBlock,
}
import com.digitalasset.canton.util.collection.BoundedMap
import io.opentelemetry.api.trace.{Span, StatusCode}

class AvailabilityModuleSpanManager(maxSize: Int = 10 * 1024) {
  // This data structure will take care of evicting and ending spans that linger for too long
  // maybe because the batch never gets proposed or ordered.
  private val batchIdToSpans =
    BoundedMap[BatchId, Seq[Span]](
      maxSize,
      (_, evictedSpans) =>
        evictedSpans.foreach { span =>
          span.setStatus(StatusCode.ERROR, "Span got too old");
          span.end()
        },
    )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var currentBlockSpan: Option[Span] = None

  def trackSpanForBlock(span: Span, orderingBlock: OrderingBlock): Unit = {
    currentBlockSpan.foreach { span =>
      span.setStatus(StatusCode.ERROR, "Closing early")
      span.end()
    }
    currentBlockSpan = Some(span)
    orderingBlock.proofs
      .map(_.batchId)
      .flatMap { bid =>
        addEventToBatchSpans(bid, "Batch proposed")
        batchIdToSpans.getOrElse(bid, Seq.empty)
      }
      .foreach { batchSpan =>
        span.addLink(batchSpan.getSpanContext).discard
        batchSpan.addLink(span.getSpanContext).discard
      }
  }

  def trackSpansForBatch(batchId: BatchId, spans: Seq[Span]): Unit = {
    // avoid doing future work on spans that are not being sampled by not including them in
    val sampledSpans = spans.filter(_.isRecording)
    if (sampledSpans.nonEmpty)
      batchIdToSpans.put(batchId, sampledSpans).discard
  }

  def addEventToBatchSpans(batchId: BatchId, description: String): Unit = batchIdToSpans
    .getOrElse(batchId, Seq.empty)
    .foreach(_.addEvent(description).discard)

  def finishBlockSpan(orderedBatchIds: Seq[BatchId]): Unit = {
    orderedBatchIds.foreach(batchId => remove(batchId))
    currentBlockSpan.foreach(_.end())
    currentBlockSpan = None
  }

  private def remove(batchId: BatchId): Unit =
    batchIdToSpans
      .remove(batchId)
      .foreach(_.foreach(_.end()))

  def endSpansWithError(batchId: BatchId, message: String): Unit =
    batchIdToSpans
      .remove(batchId)
      .foreach(_.foreach { span =>
        span.setStatus(StatusCode.ERROR, message);
        span.end()
      })

}
