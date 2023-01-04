// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.collection.{IterableFactory, IterableFactoryDefaults}
import scala.collection.immutable.{AbstractSet, HashSet, Queue, StrictOptimizedSetOps}
import scala.collection.mutable.ReusableBuilder

/** Insert-ordered Set.
  *
  * Implemented as (Queue[T], HashSet[T]).
  * Asymptotics:
  *  get: O(1)
  *  insert: O(1)
  *  remove: O(n)
  */
final class InsertOrdSet[T] private (_items: Queue[T], _hashSet: HashSet[T])
    extends AbstractSet[T]
    with IterableFactoryDefaults[T, InsertOrdSet]
    with StrictOptimizedSetOps[T, InsertOrdSet, InsertOrdSet[T]]
    with Serializable {
  override def empty: InsertOrdSet[T] = InsertOrdSet.empty
  override def size: Int = _hashSet.size
  override final def iterableFactory: IterableFactory[InsertOrdSet] = InsertOrdSet

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
        _hashSet + elem,
      )

  override def excl(elem: T): InsertOrdSet[T] =
    new InsertOrdSet(
      _items.filter(elem2 => elem != elem2),
      _hashSet - elem,
    )
}

object InsertOrdSet extends IterableFactory[InsertOrdSet] {
  type Factory[A] = Unit

  def canBuildFrom[A]: Factory[A] = ()

  def from[T](it: IterableOnce[T]): InsertOrdSet[T] = {
    it match {
      case s: InsertOrdSet[T] => s
      case _ => (newBuilder[T] ++= it).result()
    }
  }

  def newBuilder[T]: ReusableBuilder[T, InsertOrdSet[T]] = new InsertOrdSetBuilder[T]

  private val Empty = new InsertOrdSet(Queue.empty, HashSet.empty)
  final def empty[T] = Empty.asInstanceOf[InsertOrdSet[T]]

  def fromSeq[T](s: Seq[T]): InsertOrdSet[T] =
    new InsertOrdSet(Queue(s.reverse: _*), HashSet(s: _*))

  private final class InsertOrdSetBuilder[T] extends ReusableBuilder[T, InsertOrdSet[T]] {
    var m: InsertOrdSet[T] = empty
    override def clear(): Unit = {
      m = empty
    }
    override def result(): InsertOrdSet[T] = m
    override def addOne(elem: T): this.type = {
      m = m.incl(elem)
      this
    }
  }
}
