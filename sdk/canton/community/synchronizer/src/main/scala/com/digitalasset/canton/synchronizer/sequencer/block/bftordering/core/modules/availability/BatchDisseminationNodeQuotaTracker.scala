// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId

import scala.collection.mutable

/** Used for keeping track of how many batches have been stored but not ordered or expired yet per
  * node, so that after some configurable quota, we stop accepting new ones. This quota protects
  * against peers completely filling up the local database with garbage batches it is disseminating.
  */
class BatchDisseminationNodeQuotaTracker {
  private val quotas: mutable.Map[BftNodeId, Int] = mutable.Map()
  private val batches: mutable.Map[BatchId, (BftNodeId, EpochNumber)] = mutable.Map()

  private val epochs: mutable.SortedMap[EpochNumber, Set[BatchId]] = mutable.SortedMap()
  private val expiredEpochs: mutable.SortedMap[EpochNumber, Set[BatchId]] = mutable.SortedMap()

  def canAcceptForNode(node: BftNodeId, batchId: BatchId, quotaSize: Int): Boolean =
    // if we're seeing again a batch we've accepted before, we accept it again (regardless of quota having been reached)
    // because this can be the case where the originator changed topology and needs to re-collect acks
    batches.contains(batchId) || quotas.getOrElse(node, 0) < quotaSize

  def addBatch(node: BftNodeId, batchId: BatchId, batchEpoch: EpochNumber): Unit =
    if (!batches.contains(batchId)) {
      quotas.put(node, quotas.getOrElse(node, 0) + 1).discard
      epochs.put(batchEpoch, epochs.getOrElse(batchEpoch, Set()) + batchId).discard
      batches.put(batchId, (node, batchEpoch)).discard
    }

  def removeOrderedBatch(batchId: BatchId): Unit =
    batches.remove(batchId).foreach { case (node, epochNumber) =>
      quotas.updateWith(node)(_.map(_ - 1)).discard
      epochs.updateWith(epochNumber)(_.map(_ - batchId)).discard
    }

  /** Tell the tracker that an epoch has expired so that batches from that epoch and below can be
    * removed from quotas. We continue to keep track of batches from expired epochs to eventually
    * evict them. But we only keep track of expired epochs after the initial epoch, because after a
    * crash, we can't know for sure whether disseminated batches with earlier epochs have already
    * been ordered or not.
    */
  def expireEpoch(
      initialEpoch: EpochNumber,
      expirationEpoch: EpochNumber,
  ): Unit =
    epochs
      .rangeTo(expirationEpoch)
      .foreach { case (epochNumber, expiredBatches) =>
        if (epochNumber > initialEpoch) expiredEpochs.put(epochNumber, expiredBatches).discard
        epochs.remove(epochNumber).discard
        expiredBatches.foreach { expiredBatchId =>
          batches.remove(expiredBatchId).foreach { case (node, _) =>
            quotas.updateWith(node)(_.map(_ - 1)).discard
          }
        }
      }

  /** Evict batches for expired epochs whose epoch number are at or below the given eviction epoch.
    * The reason we don't immediately evict them as soon as they expire is that the output module
    * could be slightly behind the consensus module, so we would run the risk of removing a batch
    * that the output module would still fetch. By choosing the eviction epoch to be earlier than
    * the expiration epoch, we allow some room for making sure that does not happen.
    */
  def evictBatches(
      evictionEpoch: EpochNumber
  ): Seq[BatchId] = {
    val range = expiredEpochs.rangeTo(evictionEpoch)
    val batchesToEvict = range.values.toSeq.flatten
    range.keySet.foreach(expiredEpochs.remove(_).discard)
    batchesToEvict
  }
}
