// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.collection

import cats.syntax.functor.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.collection.FairBoundedQueue.EnqueueResult
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
) {

  private val nodeQueues = mutable.Map[BftNodeId, mutable.Queue[ItemType]]()
  private val arrivalOrder =
    // Uses `DropNewest` and then handles the configured drop strategy separately.
    new BoundedQueue[BftNodeId](maxQueueSize, BoundedQueue.DropStrategy.DropNewest)

  def enqueue(nodeId: BftNodeId, item: ItemType): EnqueueResult = {
    val nodeBoundedQueue =
      nodeQueues.getOrElseUpdate(nodeId, new BoundedQueue(perNodeQuota, dropStrategy))

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
            val oldestNodeId = arrivalOrder.dequeue()
            nodeQueues(oldestNodeId).dequeue().discard
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
  }
}
