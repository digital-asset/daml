// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.DiscardOps

import scala.collection.{immutable, mutable}

object SetCover {

  def covered[A](universe: Set[A], sets: Seq[Set[A]]): Boolean = {
    val cover = sets.fold(Set.empty[A])(_ ++ _)
    cover == universe
  }

  /** Let `universe` be the union of all elements in all the sets in `sets`.
    * Selects a subset of `sets` so that their union equals `universe`, i.e. a cover as specified by [[covered]].
    * The returned cover is in general neither minimal nor unique.
    *
    * Implements the standard greedy algorithm for set cover
    * with the following optimization:
    * - Do not re-insert empty sets into the priority queue
    * - Do not update the priority queue immediately when a newly covered element is removed from the other sets.
    *   Instead, update the priority queue only when it would have been the updated set's turn without the updates.
    *   This saves unnecessary priority queue operations as a set may be updated multiple times before it would be its turn.
    *   This is normally not done in the greedy algorithms that use a bucket priority queue because bucket operations are constant-time.
    *   We however use a heap priority queue from the standard library, whose operations are logarithmic.
    */
  @SuppressWarnings(Array("org.wartremover.warts.While", "org.wartremover.warts.Var"))
  def greedy[A, Id](sets: immutable.Iterable[(Id, Set[A])]): Seq[Id] = {
    val chosenIds = Seq.newBuilder[Id]

    val priorityQueueElements = sets.filter { case (_, set) => set.nonEmpty }.map { case (b, set) =>
      new PrioQueueEntry(b, set.to(mutable.Set))
    }
    // Invariants:
    // The entries' uncovered sets never contain a element that is in a chosen set.
    // There are no entries with `sizeOnLastPrioQueueInsert` being 0
    val prioQueue = new mutable.PriorityQueue[PrioQueueEntry[A, Id]]()
    prioQueue.addAll(priorityQueueElements)

    // A map from elements to the sets that contain them.
    // This allows us to efficiently remove a newly covered element from all the sets that contain it.
    val elementToId = mutable.Map.empty[A, mutable.ArrayBuffer[PrioQueueEntry[A, Id]]]
    prioQueue.foreach { entry =>
      entry.uncovered.foreach { elem =>
        val entries =
          elementToId.getOrElseUpdate(elem, mutable.ArrayBuffer.empty[PrioQueueEntry[A, Id]])
        entries.addOne(entry)
      }
    }

    while (prioQueue.nonEmpty) {
      val entry = prioQueue.dequeue()
      val newSize = entry.uncovered.size
      if (newSize == entry.sizeOnLastPrioQueueInsert) {
        chosenIds += entry.id
        entry.uncovered.foreach { elem =>
          elementToId.remove(elem).foreach { entriesToRemoveTheElementFrom =>
            entriesToRemoveTheElementFrom.foreach { entryToRemoveTheElementFrom =>
              if (entryToRemoveTheElementFrom != entry) {
                entryToRemoveTheElementFrom.uncovered.remove(elem).discard[Boolean]
              }
            }
          }
        }
      } else if (newSize > 0) {
        entry.updateSizeCache()
        prioQueue.enqueue(entry)
      }
    }
    chosenIds.result()
  }

  /** Entry for the priority queue of the greedy set cover algorithm
    * @param id The identifier for a set
    * @param uncovered The set of elements in the identified sets that are not in an already chosen set
    */
  private class PrioQueueEntry[A, Id](
      val id: Id,
      val uncovered: mutable.Set[A],
  ) {

    /** Used to determine the order in the priority queue.
      * Must not be modified while the entry is in the priority queue
      */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private[this] var sizeOnLastPrioQueueInsertVar: Int = uncovered.size

    def sizeOnLastPrioQueueInsert: Int = sizeOnLastPrioQueueInsertVar

    def updateSizeCache(): Unit = {
      sizeOnLastPrioQueueInsertVar = uncovered.size
    }
  }
  private object PrioQueueEntry {
    private val orderingSetAndUncoveredAny: Ordering[PrioQueueEntry[Any, Any]] =
      Ordering.by(_.sizeOnLastPrioQueueInsert)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def orderingSetAndUncovered[A, Id]: Ordering[PrioQueueEntry[A, Id]] =
      orderingSetAndUncoveredAny.asInstanceOf[Ordering[PrioQueueEntry[A, Id]]]
  }
}
