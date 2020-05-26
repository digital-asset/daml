// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

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
import scala.collection.{SetLike, AbstractSet}
import scala.collection.generic.{
  ImmutableSetFactory,
  GenericCompanion,
  CanBuildFrom,
  GenericSetTemplate
}

final class InsertOrdSet[T] private (_items: Queue[T], _hashSet: HashSet[T])
    extends AbstractSet[T]
    with Set[T]
    with SetLike[T, InsertOrdSet[T]]
    with GenericSetTemplate[T, InsertOrdSet]
    with Serializable {
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
      new InsertOrdSet(
        elem +: _items,
        _hashSet + elem
      )

  override def -(elem: T): InsertOrdSet[T] =
    new InsertOrdSet(
      _items.filter(elem2 => elem != elem2),
      _hashSet - elem
    )

  override def companion: GenericCompanion[InsertOrdSet] = InsertOrdSet

}

object InsertOrdSet extends ImmutableSetFactory[InsertOrdSet] {
  private val Empty = new InsertOrdSet(Queue.empty, HashSet.empty)
  override def empty[T] = Empty.asInstanceOf[InsertOrdSet[T]]
  def emptyInstance: InsertOrdSet[Any] = empty[Any]

  def fromSeq[T](s: Seq[T]): InsertOrdSet[T] =
    new InsertOrdSet(Queue(s.reverse: _*), HashSet(s: _*))

  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, InsertOrdSet[A]] =
    setCanBuildFrom[A]

}
