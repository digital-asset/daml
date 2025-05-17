// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.collection

import cats.syntax.functor.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.collection.FairBoundedQueue.{
  DeduplicationStrategy,
  EnqueueResult,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.util.collection.BoundedQueue
import com.google.common.annotations.VisibleForTesting

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
) {

  private val nodeQueues = mutable.Map[BftNodeId, mutable.Queue[ItemType]]()
  private val arrivalOrder =
    // Use `DropNewest` and handle the configured drop strategy separately, so that a node cannot use other nodes'
    //  quotas when exceeding the total capacity.
    new BoundedQueue[BftNodeId](maxQueueSize, BoundedQueue.DropStrategy.DropNewest)

  def enqueue(nodeId: BftNodeId, item: ItemType): EnqueueResult = {
    val nodeBoundedQueue =
      nodeQueues.getOrElseUpdate(nodeId, new BoundedQueue(perNodeQuota, dropStrategy))

    deduplicationStrategy match {
      case DeduplicationStrategy.PerNode =>
        if (nodeBoundedQueue.contains(item)) {
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
  ) = {
    // `ArrayDeque` is the underlying collection and its `size` implementation has constant time complexity.
    val originalNodeQueueSize = nodeBoundedQueue.size
    val originalArrivalQueueSize = arrivalOrder.size

    val nodeBoundedQueueWasFull = nodeBoundedQueue.enqueue(item).sizeIs == originalNodeQueueSize

    if (nodeBoundedQueueWasFull) {
      dropStrategy match {
        case BoundedQueue.DropStrategy.DropOldest =>
          arrivalOrder.removeFirst(otherNodeId => otherNodeId == nodeId).discard
          arrivalOrder.enqueue(nodeId).discard
        case BoundedQueue.DropStrategy.DropNewest => // nothing to change
      }
      EnqueueResult.PerNodeQuotaExceeded(nodeId)
    } else {
      val totalCapacityWasExceeded =
        arrivalOrder.enqueue(nodeId).sizeIs == originalArrivalQueueSize
      if (totalCapacityWasExceeded) {
        dropStrategy match {
          case BoundedQueue.DropStrategy.DropOldest =>
            // There's always at least one (just inserted) element.
            nodeBoundedQueue.removeHead().discard
            arrivalOrder.removeFirst(otherNodeId => otherNodeId == nodeId).discard
            arrivalOrder.enqueue(nodeId).discard
          case BoundedQueue.DropStrategy.DropNewest =>
            nodeBoundedQueue.removeLast().discard
        }
        EnqueueResult.TotalCapacityExceeded
      } else {
        EnqueueResult.Success
      }
    }
  }

  def dequeue(): Option[ItemType] =
    if (arrivalOrder.isEmpty) {
      None
    } else {
      val nodeId = arrivalOrder.dequeue()
      val nodeQueue = nodeQueues(nodeId)
      val item = nodeQueue.dequeue()
      if (nodeQueue.isEmpty) {
        nodeQueues.remove(nodeId).discard
      }
      Some(item)
    }

  def dequeueAll(predicate: ItemType => Boolean): Seq[ItemType] = {
    val (dequeuedItems, remainingNodesToItems) =
      arrivalOrder.foldLeft((Seq[ItemType](), Seq[(BftNodeId, ItemType)]())) {
        case ((dequeuedItems, remainingNodesToItems), nodeId) =>
          val item = nodeQueues(nodeId).dequeue()
          if (predicate(item)) {
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
  def dump: Seq[ItemType] = {
    val nodesToItemQueueCopies = nodeQueues.toMap.fmap(_.clone())
    arrivalOrder.view.map { nodeId =>
      nodesToItemQueueCopies(nodeId).dequeue()
    }.toSeq
  }
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
    case object PerNode extends DeduplicationStrategy
  }
}
