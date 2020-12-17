// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.collection.immutable.{HashSet, Queue}

/**
  * Insert-ordered Set.
  *
  * Implemented as (Queue[T], HashSet[T]).
  * Asymptotics:
  *  get: O(1)
  *  insert: O(1)
  *  remove: O(n)
  */
final class InsertOrdSet[T] private (_items: Queue[T], _hashSet: HashSet[T])
    extends AbstractInsertOrdSet[T] {
  override def empty: InsertOrdSet[T] = InsertOrdSet.empty
  override def size: Int = _hashSet.size

  def iterator: Iterator[T] =
    _items.reverseIterator

  override def contains(elem: T): Boolean =
    _hashSet.contains(elem)

  override def incl(elem: T): InsertOrdSet[T] =
    if (_hashSet.contains(elem))
      this
    else
      new InsertOrdSet(
        elem +: _items,
        _hashSet + elem
      )

  override def excl(elem: T): InsertOrdSet[T] =
    new InsertOrdSet(
      _items.filter(elem2 => elem != elem2),
      _hashSet - elem
    )
}

object InsertOrdSet extends InsertOrdSetCompanion {
  private val Empty = new InsertOrdSet(Queue.empty, HashSet.empty)
  override def empty[T] = Empty.asInstanceOf[InsertOrdSet[T]]

  def fromSeq[T](s: Seq[T]): InsertOrdSet[T] =
    new InsertOrdSet(Queue(s.reverse: _*), HashSet(s: _*))

  // Here only for 2.12 (harmless in 2.13); placed in InsertOrdSetCompanion the
  // implicit gets in an unwinnable fight with Set's version
  implicit override def canBuildFrom[A]: Factory[A] =
    super.canBuildFrom

}
