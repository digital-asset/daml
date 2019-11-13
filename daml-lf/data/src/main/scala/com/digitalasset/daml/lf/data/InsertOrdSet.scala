// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

/**
  * Insert-ordered Set.
  *
  * Implemented as (Queue[T], HashSet[T]).
  * Asymptotics:
  *  get: O(1)
  *  insert: O(1)
  *  remove: O(n)
  */
import scala.collection.immutable.{HashSet, Set, Queue}

sealed abstract class InsertOrdSet[T] extends Set[T] {
  def _items: Queue[T]
  def _hashSet: HashSet[T]

  override def empty: InsertOrdSet[T] = InsertOrdSet.empty

  override def size: Int = _hashSet.size

  def iterator: Iterator[T] =
    _items.reverseIterator

  override def contains(elem: T): Boolean =
    _hashSet.contains(elem)

  override def +(elem: T): InsertOrdSet[T] =
    if (_hashSet.contains(elem))
      this
    else
      NonEmptyInsertOrdSet(
        elem +: _items,
        _hashSet + elem
      )

  override def -(elem: T): InsertOrdSet[T] =
    NonEmptyInsertOrdSet(
      _items.filter(elem2 => elem != elem2),
      _hashSet - elem
    )
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final case object EmptyInsertOrdSet extends InsertOrdSet[Any] {
  override def _items = Queue[Any]()
  override def _hashSet = HashSet[Any]()
}

final case class NonEmptyInsertOrdSet[T](
    override val _items: Queue[T],
    override val _hashSet: HashSet[T])
    extends InsertOrdSet[T]

object InsertOrdSet {
  def empty[T] = EmptyInsertOrdSet.asInstanceOf[InsertOrdSet[T]]

  def fromSet[T](s: Set[T]): InsertOrdSet[T] =
    NonEmptyInsertOrdSet(
      s.to[Queue],
      s.to[HashSet]
    )

  def fromSeq[T](s: Seq[T]): InsertOrdSet[T] =
    NonEmptyInsertOrdSet(Queue(s.reverse: _*), HashSet(s: _*))
}
