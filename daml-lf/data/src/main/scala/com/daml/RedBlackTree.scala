// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import com.daml.scalautil.Statement.discard

import scala.collection.{immutable, mutable, IndexedSeq}

// This object implement linear time builder for immutable TreeMap and TreeSet.
// Requires the entries to be ordered, crashes if not.
// Relies on the fact that TreeMap.from (resp TreeMap.from) is linear when its
// argument is a SortedMap (resp. SortedSet) with the same ordering.
private[data] object RedBlackTree {

  private[this] final class FakeSortedSet[X](
      override val ordering: Ordering[X],
      sorted: IndexedSeq[X],
  ) extends immutable.SortedSet[X] {
    override def iterator: Iterator[X] = sorted.iterator
    override def size: Int = sorted.size

    // As for scala 2.12.10 immutable.TreeSet.from does not use the following methods
    override def incl(elem: X): Nothing = ???
    override def excl(elem: X): Nothing = ???
    override def iteratorFrom(start: X): Nothing = ???
    override def contains(elem: X): Nothing = ???
    override def rangeImpl(from: Option[X], until: Option[X]): Nothing = ???
  }

  private[this] final class FakeSortedMap[K, V](
      override val ordering: Ordering[K],
      sorted: IndexedSeq[(K, V)],
  ) extends immutable.SortedMap[K, V] {
    override def iterator: Iterator[(K, V)] = sorted.iterator
    override def size: Int = sorted.size

    // As for scala 2.12.10 immutable.TreeMap.from does not use the following methods
    override def removed(key: K): Nothing = ???
    override def updated[V1 >: V](key: K, value: V1): Nothing = ???
    override def iteratorFrom(start: K): Nothing = ???
    override def keysIteratorFrom(start: K): Nothing = ???
    override def rangeImpl(from: Option[K], until: Option[K]): Nothing = ???
    override def get(key: K): Nothing = ???
  }

  private[this] def toOrderIndexedSeq[X](
      lessThan: (X, X) => Boolean,
      orderedEntries: IterableOnce[X],
  ): IndexedSeq[X] = {
    val it = orderedEntries.iterator
    if (it.isEmpty)
      Vector.empty
    else {
      val buffer = new mutable.ArrayBuffer[X]()
      buffer.sizeHint(orderedEntries.knownSize)
      var previous = it.next()
      it.foreach { next =>
        if (lessThan(previous, next))
          discard(buffer.addOne(previous))
        previous = next
      }
      val _ = buffer.addOne(previous)
      buffer
    }
  }

  private[this] def lenientBehaviour[X](ordering: Ordering[X])(x1: X, x2: X): Boolean =
    ordering.compare(x1, x2).sign match {
      case -1 => true
      case 0 => false
      case _ => throw new IllegalArgumentException("the entries are not ordered")
    }

  private[this] def strictBehaviour[X](ordering: Ordering[X])(x1: X, x2: X): Boolean =
    ordering.compare(x1, x2).sign match {
      case -1 => true
      case _ => throw new IllegalArgumentException("the entries are not strictly ordered")
    }

  def adapt[K, V](behaviour: (K, K) => Boolean): ((K, V), (K, V)) => Boolean = {
    case ((k1, _), (k2, _)) => behaviour(k1, k2)
  }

  private[data] def treeMapFromOrderedEntries[K, V](
      entries: IterableOnce[(K, V)]
  )(implicit ordering: Ordering[K]): immutable.TreeMap[K, V] =
    entries match {
      case treeMap: immutable.TreeMap[K, V] if treeMap.ordering == ordering => treeMap
      case _ =>
        val seq = toOrderIndexedSeq(adapt(lenientBehaviour(ordering)), entries)
        immutable.TreeMap.from(new FakeSortedMap(ordering, seq))
    }

  private[data] def treeMapFromStrictlyOrderedEntries[K, V](
      entries: IterableOnce[(K, V)]
  )(implicit ordering: Ordering[K]): immutable.TreeMap[K, V] =
    entries match {
      case treeMap: immutable.TreeMap[K, V] if treeMap.ordering == ordering => treeMap
      case _ =>
        val seq = toOrderIndexedSeq(adapt(strictBehaviour(ordering)), entries)
        immutable.TreeMap.from(new FakeSortedMap(ordering, seq))
    }

  private[data] def treeSetFromOrderedEntries[X](
      entries: IterableOnce[X]
  )(implicit ordering: Ordering[X]): immutable.TreeSet[X] =
    entries match {
      case treeSet: immutable.TreeSet[X] if treeSet.ordering == ordering => treeSet
      case _ =>
        val seq = toOrderIndexedSeq(lenientBehaviour(ordering), entries)
        immutable.TreeSet.from(new FakeSortedSet(ordering, seq))
    }

  private[data] def treeSetFromStrictlyOrderedEntries[X](
      entries: IterableOnce[X]
  )(implicit ordering: Ordering[X]): immutable.TreeSet[X] =
    entries match {
      case treeSet: immutable.TreeSet[X] if treeSet.ordering == ordering => treeSet
      case _ =>
        val seq = toOrderIndexedSeq(strictBehaviour(ordering), entries)
        immutable.TreeSet.from(new FakeSortedSet(ordering, seq))
    }

}

object TreeMap {
  /* Linear time builder from TreeMap.
   * Requires the entries to be ordered, crashes if not.
   * Like normal TreeMap builder, in case of duplicate key, the last wins
   */
  @throws[IllegalArgumentException]
  def fromOrderedEntries[K, V](
      entries: IterableOnce[(K, V)]
  )(implicit order: Ordering[K]): immutable.TreeMap[K, V] =
    RedBlackTree.treeMapFromOrderedEntries(entries)

  /** Like fromOrderedEntries but crashes in case of duplicate key
    */
  @throws[IllegalArgumentException]
  def fromStrictlyOrderedEntries[K, V](
      entries: IterableOnce[(K, V)]
  )(implicit order: Ordering[K]): immutable.TreeMap[K, V] =
    RedBlackTree.treeMapFromStrictlyOrderedEntries(entries)
}

object TreeSet {
  /* Linear time builder from TreeSet.
   * Requires the entries to be ordered, crashes if not.
   * Like normal TreeSet builder, in case of duplicate key, the last wins
   */
  @throws[IllegalArgumentException]
  def fromOrderedEntries[X](
      entries: IterableOnce[X]
  )(implicit order: Ordering[X]): immutable.TreeSet[X] =
    RedBlackTree.treeSetFromOrderedEntries(entries)

  /** Like fromOrderedEntries but crashes in case of duplicate entries
    */
  @throws[IllegalArgumentException]
  def fromStrictlyOrderedEntries[X](
      entries: IterableOnce[X]
  )(implicit order: Ordering[X]): immutable.TreeSet[X] =
    RedBlackTree.treeSetFromStrictlyOrderedEntries(entries)

}
