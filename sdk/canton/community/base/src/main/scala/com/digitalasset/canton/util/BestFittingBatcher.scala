// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.BestFittingBatcher.{CapacityLeft, Sized}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.IterableOps
import scala.collection.immutable.SortedMap

/** A mutable, thread-safe (and lock-free) batcher that groups items into batches such that the
  * batches are as full as possible and a set of items is always kept together in a batch.
  *
  * It uses a best-fitting algorithm (https://en.wikipedia.org/wiki/Best-fit_bin_packing) to place
  * items into existing batches or create new batches as needed.
  *
  * @param maxBatchSize
  *   The maximum batch size
  * @tparam ItemsT
  *   The type of items to be batched. Must implement `Sized` to provide size information.
  */
class BestFittingBatcher[ItemsT <: Sized](maxBatchSize: PositiveInt) {

  private type BatchT = NonEmpty[Vector[ItemsT]]

  private val stateRef: AtomicReference[SortedMap[CapacityLeft, NonEmpty[Vector[BatchT]]]] =
    new AtomicReference(SortedMap.empty[CapacityLeft, NonEmpty[Vector[BatchT]]])

  /** Inserts a set of items together into the fullest batch that can contain them.
    *
    * @param items
    *   The items to be inserted
    * @return
    *   `true` if the items were inserted successfully, `false` if the items exceed the maximum
    *   batch size
    */
  def add(items: ItemsT): Boolean =
    if (items.sizeIs > maxBatchSize.value) {
      false
    } else {
      stateRef.updateAndGet { batches =>
        batches.iteratorFrom(items.size.value).nextOption() match {

          case Some((capacityLeft, batchesWithCapacityLeft)) =>
            val (head, updatedBatchesWithUpdatedCapacityLeft) =
              pollHead(batches, capacityLeft, batchesWithCapacityLeft)
            val updatedBatch = head :+ items
            val newCapacityLeft = capacityLeft - items.size.value
            updatedBatchesWithUpdatedCapacityLeft.updatedWith(newCapacityLeft) {
              case Some(existingBatches) => Some(existingBatches :+ updatedBatch)
              case None => Some(NonEmpty(Vector, updatedBatch))
            }

          case None =>
            val newBatch = NonEmpty(Vector, items)
            val newCapacityLeft = maxBatchSize.value - items.size.value
            batches.updatedWith(newCapacityLeft) {
              case Some(existingBatches) => Some(existingBatches :+ newBatch)
              case None => Some(NonEmpty(Vector, newBatch))
            }
        }
      }.discard
      true
    }

  /** Extracts the fullest pending batch, if any.
    */
  def poll(): Option[BatchT] =
    AtomicUtil.updateAndGetComputed(stateRef) { batches =>
      batches.headOption.fold(batches -> Option.empty[BatchT]) {
        case (capacityLeft, fullestBatches) =>
          val (head, newBatches) = pollHead(batches, capacityLeft, fullestBatches)
          newBatches -> Some(head)
      }
    }

  private def pollHead(
      batches: SortedMap[CapacityLeft, NonEmpty[Vector[BatchT]]],
      capacityLeft: CapacityLeft,
      batchesWithCapacityLeft: NonEmpty[Vector[BatchT]],
  ): (BatchT, SortedMap[CapacityLeft, NonEmpty[Vector[BatchT]]]) =
    batchesWithCapacityLeft.head1 -> {
      NonEmpty.from(batchesWithCapacityLeft.tail1) match {
        case Some(batchesWithCapacityLeftNE) =>
          batches.updated(capacityLeft, batchesWithCapacityLeftNE)
        case None =>
          batches.removed(capacityLeft)
      }
    }
}

object BestFittingBatcher {

  type CapacityLeft = Int

  trait Sized {
    def size: PositiveInt
    def sizeIs: IterableOps.SizeCompareOps
  }
}
