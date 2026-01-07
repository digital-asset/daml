// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import cats.syntax.functor.*
import com.daml.metrics.api.MetricHandle.{Gauge, Meter}
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.FairBoundedQueue.{
  DeduplicationStrategy,
  EnqueueResult,
}
import com.digitalasset.canton.util.collection.BoundedQueue
import com.google.common.annotations.VisibleForTesting

import java.time.Instant
import scala.collection.mutable

/** Provides fairness by having a per-node quota while preserving the original arrival order on top
  * of the bare [[com.digitalasset.canton.util.collection.BoundedQueue]]. For simplicity/safety does
  * not implement any collection-like interface. The implementation is not thread-safe.
  */
class FairBoundedQueue[ItemType](
    maxQueueSize: Int,
    perNodeQuota: Int,
    dropStrategy: BoundedQueue.DropStrategy = BoundedQueue.DropStrategy.DropOldest,
    deduplicationStrategy: DeduplicationStrategy = DeduplicationStrategy.Noop,
    metrics: Option[BftOrderingMetrics] = None,
    sizeGauge: Option[Gauge[Int]] = None,
    maxSizeGauge: Option[Gauge[Int]] = None,
    dropMeter: Option[Meter] = None,
    orderingStageLatencyLabel: Option[String] = None,
)(implicit metricsContext: MetricsContext) {

  private val nodeQueues = mutable.Map[BftNodeId, mutable.Queue[ItemType]]()
  private val arrivalOrder =
    // Use `DropNewest` and handle the configured drop strategy separately, so that a node cannot use other nodes'
    //  quotas when exceeding the total capacity.
    new BoundedQueue[(BftNodeId, Instant)](maxQueueSize, BoundedQueue.DropStrategy.DropNewest)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var maxSizeOverall: Int = 0

  // Explicitly initialize the gauges and meters in order to provide the correct metrics context, which
  //  includes the reporting sequencer.
  sizeGauge.foreach(_.updateValue(0))
  maxSizeGauge.foreach(_.updateValue(0))
  dropMeter.foreach(_.mark(0))
  deduplicationStrategy match {
    case DeduplicationStrategy.PerNode(duplicatedMeter) =>
      duplicatedMeter.foreach(_.mark(0))
    case DeduplicationStrategy.Noop =>
  }

  def enqueue(nodeId: BftNodeId, item: ItemType)(implicit
      metricsContext: MetricsContext
  ): EnqueueResult = {
    val nodeBoundedQueue =
      nodeQueues.getOrElseUpdate(nodeId, new BoundedQueue(perNodeQuota, dropStrategy))

    deduplicationStrategy match {
      case DeduplicationStrategy.PerNode(duplicateMeter) =>
        if (nodeBoundedQueue.contains(item)) {
          duplicateMeter.foreach(_.mark())
          EnqueueResult.Duplicate(nodeId)
        } else {
          actuallyEnqueue(nodeId, item, nodeBoundedQueue)
        }
      case DeduplicationStrategy.Noop =>
        actuallyEnqueue(nodeId, item, nodeBoundedQueue)
    }
  }

  private def actuallyEnqueue(
      nodeId: BftNodeId,
      item: ItemType,
      nodeBoundedQueue: mutable.Queue[ItemType],
  )(implicit metricsContext: MetricsContext): EnqueueResult = {
    // `ArrayDeque` is the underlying collection and its `size` implementation has constant time complexity.
    val originalNodeQueueSize = nodeBoundedQueue.size
    val originalArrivalQueueSize = arrivalOrder.size

    val nodeBoundedQueueWasFull = nodeBoundedQueue.enqueue(item).sizeIs == originalNodeQueueSize

    if (nodeBoundedQueueWasFull) {
      dropMeter.foreach(_.mark())
      dropStrategy match {
        case BoundedQueue.DropStrategy.DropOldest =>
          removeFirstArrivedFrom(nodeId)
          arrivalOrder.enqueue(nodeId -> Instant.now()).discard
        case BoundedQueue.DropStrategy.DropNewest => // nothing to change
      }
      EnqueueResult.PerNodeQuotaExceeded(nodeId)
    } else {
      val totalCapacityWasExceeded =
        arrivalOrder.enqueue(nodeId -> Instant.now()).sizeIs == originalArrivalQueueSize
      if (totalCapacityWasExceeded) {
        dropMeter.foreach(_.mark())
        dropStrategy match {
          case BoundedQueue.DropStrategy.DropOldest =>
            // There's always at least one (just inserted) element.
            nodeBoundedQueue.removeHead().discard
            removeFirstArrivedFrom(nodeId)
            arrivalOrder.enqueue(nodeId -> Instant.now()).discard
          case BoundedQueue.DropStrategy.DropNewest =>
            nodeBoundedQueue.removeLast().discard
        }
        EnqueueResult.TotalCapacityExceeded
      } else {
        sizeGauge.foreach(_.updateValue(size + 1))
        if (size > maxSizeOverall) {
          maxSizeOverall = size
          maxSizeGauge.foreach(_.updateValue(maxSizeOverall))
        }
        EnqueueResult.Success
      }
    }
  }

  def dequeue()(implicit metricsContext: MetricsContext): Option[ItemType] =
    if (arrivalOrder.isEmpty) {
      None
    } else {
      sizeGauge.foreach(_.updateValue(size - 1))
      val (nodeId, enqueuedAt) = arrivalOrder.dequeue()
      emitOrderingStageLatency(enqueuedAt)
      val nodeQueue = nodeQueues(nodeId)
      val item = nodeQueue.dequeue()
      if (nodeQueue.isEmpty)
        nodeQueues.remove(nodeId).discard
      Some(item)
    }

  def dequeueAll(
      predicate: ItemType => Boolean
  )(implicit metricsContext: MetricsContext): Seq[ItemType] = {
    val (dequeuedItems, remainingNodesToItems) =
      arrivalOrder.foldLeft((Seq[ItemType](), Seq[(BftNodeId, ItemType)]())) {
        case (dequeuedItems -> remainingNodesToItems, nodeId -> enqueuedAt) =>
          val item = nodeQueues(nodeId).dequeue()
          if (predicate(item)) {
            sizeGauge.foreach(_.updateValue(size - 1))
            emitOrderingStageLatency(enqueuedAt)
            (dequeuedItems :+ item, remainingNodesToItems)
          } else {
            (dequeuedItems, remainingNodesToItems :+ (nodeId -> item))
          }
      }

    // Rebuild the underlying structures.
    arrivalOrder.clear()
    remainingNodesToItems.foreach { case (nodeId, item) =>
      enqueue(nodeId, item).discard
    }

    dequeuedItems
  }

  def size: Int = arrivalOrder.size

  @VisibleForTesting
  private[bftordering] def dump: Seq[ItemType] = {
    val nodesToItemQueueCopies = nodeQueues.toMap.fmap(_.clone())
    arrivalOrder.view.map { case (nodeId, _) =>
      nodesToItemQueueCopies(nodeId).dequeue()
    }.toSeq
  }

  private def emitOrderingStageLatency(
      enqueuedAt: Instant
  )(implicit metricsContext: MetricsContext): Unit =
    metrics.foreach { metrics =>
      orderingStageLatencyLabel.foreach(label =>
        metrics.performance.orderingStageLatency
          .emitOrderingStageLatency(label, Some(enqueuedAt))
      )
    }

  private def removeFirstArrivedFrom(bftNodeId: BftNodeId): Unit =
    arrivalOrder.removeFirst { case (nodeId, _) => nodeId == bftNodeId }.discard
}

object FairBoundedQueue {
  sealed trait EnqueueResult extends Product with Serializable
  object EnqueueResult {
    case object Success extends EnqueueResult
    case object TotalCapacityExceeded extends EnqueueResult
    final case class PerNodeQuotaExceeded(nodeId: BftNodeId) extends EnqueueResult
    final case class Duplicate(nodeId: BftNodeId) extends EnqueueResult
  }

  sealed trait DeduplicationStrategy extends Product with Serializable
  object DeduplicationStrategy {
    case object Noop extends DeduplicationStrategy
    final case class PerNode(duplicatedMeter: Option[Meter] = None) extends DeduplicationStrategy
  }
}
