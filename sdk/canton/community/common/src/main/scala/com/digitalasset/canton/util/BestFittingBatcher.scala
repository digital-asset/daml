// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.BestFittingBatcher.CapacityLeft

import java.util.concurrent.atomic.AtomicReference
import scala.collection.IterableOps
import scala.collection.immutable.SortedMap

import BestFittingBatcher.Sized

class BestFittingBatcher[ItemsT <: Sized](maxBatchSize: Int) {

  private type BatchT = NonEmpty[Vector[ItemsT]]

  private val stateRef =
    new AtomicReference(
      Option.empty[BatchT] -> SortedMap.empty[CapacityLeft, NonEmpty[Vector[BatchT]]]
    )

  def add(items: ItemsT): Boolean =
    if (items.sizeIs > maxBatchSize) {
      false
    } else {
      stateRef.updateAndGet { case (extracted, batches) =>
        batches.iteratorFrom(items.size).nextOption() match {

          case Some((capacityLeft, batchesWithCapacityLeft)) =>
            val (head, updatedBatchesWithUpdatedCapacityLeft) =
              pollHead(batches, capacityLeft, batchesWithCapacityLeft)
            val updatedBatch = head :+ items
            val newCapacityLeft = capacityLeft - items.size
            val updatedBatchesWithNewCapacityLeft =
              updatedBatchesWithUpdatedCapacityLeft.updatedWith(newCapacityLeft) {
                case Some(existingBatches) =>
                  Some(existingBatches :+ updatedBatch)
                case None =>
                  Some(NonEmpty(Vector, updatedBatch))
              }
            (extracted, updatedBatchesWithNewCapacityLeft)

          case None =>
            val newBatch = NonEmpty(Vector, items)
            val newCapacityLeft = maxBatchSize - items.size
            val updatedBatches =
              batches.updatedWith(newCapacityLeft) {
                case Some(existingBatches) =>
                  Some(existingBatches :+ newBatch)
                case None =>
                  Some(NonEmpty(Vector, newBatch))
              }
            (extracted, updatedBatches)
        }
      }.discard
      true
    }

  def poll(): Option[BatchT] =
    stateRef.updateAndGet { case (extracted, packings) =>
      packings.headOption
        .fold(Option.empty[BatchT] -> packings) { case (capacityLeft, fullestPackings) =>
          val (head, newPackings) = pollHead(packings, capacityLeft, fullestPackings)
          Some(head) -> newPackings
        }
    }._1

  private def pollHead(
      packings: SortedMap[CapacityLeft, NonEmpty[Vector[BatchT]]],
      capacityLeft: CapacityLeft,
      packingsWithCapacityLeft: NonEmpty[Vector[BatchT]],
  ): (BatchT, SortedMap[CapacityLeft, NonEmpty[Vector[BatchT]]]) =
    packingsWithCapacityLeft.head1 -> {
      NonEmpty.from(packingsWithCapacityLeft.tail1) match {
        case Some(packingsWithCapacityLeftNE) =>
          packings.updated(capacityLeft, packingsWithCapacityLeftNE)
        case None =>
          packings.removed(capacityLeft)
      }
    }
}

object BestFittingBatcher {

  type CapacityLeft = Int

  trait Sized {
    def size: Int
    def sizeIs: IterableOps.SizeCompareOps
  }
}
