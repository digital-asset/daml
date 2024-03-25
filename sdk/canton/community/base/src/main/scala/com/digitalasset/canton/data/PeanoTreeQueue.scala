// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.data.PeanoQueue.{
  AssociatedValue,
  BeforeHead,
  InsertedValue,
  NotInserted,
}
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable
import scala.concurrent.blocking

/** Implementation of [[PeanoQueue]] for [[Counter]] keys based on a tree map.
  *
  * This implementation is not thread safe.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class PeanoTreeQueue[Discr, V](initHead: Counter[Discr]) extends PeanoQueue[Counter[Discr], V] {

  private val elems: mutable.TreeMap[Counter[Discr], V] = mutable.TreeMap.empty[Counter[Discr], V]

  private var headV: Counter[Discr] = initHead

  override def head: Counter[Discr] = headV

  private var frontV: Counter[Discr] = initHead

  override def front: Counter[Discr] = frontV

  override def insert(key: Counter[Discr], value: V): Boolean = {
    require(
      key.isNotMaxValue,
      s"The maximal key value ${Counter.MaxValue} cannot be inserted.",
    )

    def associationChanged(oldValue: V): Nothing =
      throw new IllegalArgumentException(
        s"New value $value for key $key differs from old value $oldValue."
      )

    if (key >= frontV) {
      elems.put(key, value) match {
        case None => if (key == frontV) cleanup()
        case Some(oldValue) =>
          if (oldValue != value) {
            elems.put(key, oldValue).discard // undo the changes
            associationChanged(oldValue)
          }
      }
      true
    } else if (key >= headV) {
      val oldValue =
        elems
          .get(key)
          .getOrElse(
            throw new IllegalStateException("Unreachable code by properties of the PeanoQueue")
          )
      if (value != oldValue)
        associationChanged(oldValue)
      true
    } else false
  }

  override def alreadyInserted(key: Counter[Discr]): Boolean = {
    if (key >= frontV) {
      elems.contains(key)
    } else {
      true
    }
  }

  /** Update `front` as long as the [[elems]] contain consecutive key-value pairs starting at `front`.
    */
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def cleanup(): Unit = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var next = frontV
    val iter = elems.keysIteratorFrom(next)
    while (iter.hasNext && iter.next() == next) {
      next += 1
    }
    frontV = next
  }

  def get(key: Counter[Discr]): AssociatedValue[V] = {
    if (key < headV) BeforeHead
    else
      elems.get(key) match {
        case None =>
          val floor = elems.rangeImpl(None, Some(key)).lastOption.map(_._2)
          val ceiling = elems.rangeImpl(Some(key), None).headOption.map(_._2)
          NotInserted(floor, ceiling)
        case Some(value) => InsertedValue(value)
      }
  }

  override def poll(): Option[(Counter[Discr], V)] = {
    if (headV >= frontV) None
    else {
      val key = headV
      val value =
        elems
          .remove(key)
          .getOrElse(
            throw new IllegalStateException("Unreachable code by properties of the PeanoQueue")
          )
      headV = key + 1
      Some((key, value))
    }
  }

  @VisibleForTesting
  def invariant: Boolean = {
    headV <= frontV &&
    elems.rangeImpl(None, Some(frontV + 1)).toSeq.map(_._1) == (headV until frontV)
  }

  override def toString: String = {
    val builder = new StringBuilder("PeanoQueue(front = ").append(frontV)
    elems.foreach { case (k, v) =>
      builder.append(", ").append(k).append("->").append(v.toString).discard[StringBuilder]
    }
    builder.append(")")
    builder.toString
  }
}

object PeanoTreeQueue {
  def apply[Discr, V](init: Counter[Discr]) = new PeanoTreeQueue[Discr, V](init)
}

/** A thread-safe [[PeanoTreeQueue]] thanks to synchronizing all methods */
class SynchronizedPeanoTreeQueue[Discr, V](initHead: Counter[Discr])
    extends PeanoQueue[Counter[Discr], V] {
  private[this] val queue: PeanoQueue[Counter[Discr], V] = new PeanoTreeQueue(initHead)

  override def head: Counter[Discr] = blocking { queue synchronized queue.head }

  override def front: Counter[Discr] = blocking { queue synchronized queue.front }

  override def insert(key: Counter[Discr], value: V): Boolean =
    blocking { queue synchronized queue.insert(key, value) }

  override def alreadyInserted(key: Counter[Discr]): Boolean =
    blocking { queue synchronized queue.alreadyInserted(key) }

  override def get(key: Counter[Discr]): AssociatedValue[V] = blocking {
    queue synchronized queue.get(key)
  }

  override def poll(): Option[(Counter[Discr], V)] = blocking { queue synchronized queue.poll() }

}
