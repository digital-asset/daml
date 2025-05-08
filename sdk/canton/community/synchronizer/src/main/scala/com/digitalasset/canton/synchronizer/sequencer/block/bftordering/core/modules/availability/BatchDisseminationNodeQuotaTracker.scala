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
  private val epochs: mutable.SortedMap[EpochNumber, Set[BatchId]] = mutable.SortedMap()
  private val batches: mutable.Map[BatchId, (BftNodeId, EpochNumber)] = mutable.Map()

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

  def expireEpoch(expiredEpochNumber: EpochNumber): Unit = {
    epochs
      .rangeTo(expiredEpochNumber)
      .foreach { case (_, expiredBatches) =>
        expiredBatches.foreach { expiredBatchId =>
          batches.remove(expiredBatchId).foreach { case (node, _) =>
            quotas.updateWith(node)(_.map(_ - 1)).discard
          }
        }
      }
    epochs.dropWhile { case (epochNumber, _) =>
      epochNumber <= expiredEpochNumber
    }.discard
  }
}
