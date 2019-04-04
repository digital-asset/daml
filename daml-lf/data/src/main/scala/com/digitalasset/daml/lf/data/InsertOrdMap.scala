// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

/**
  * Insert-ordered Map (like ListMap), but with efficient lookups.
  *
  * Implemented as (Queue[K], HashMap[K, V]).
  * Asymptotics:
  *  get: O(1)
  *  insert: O(1)
  *  remove: O(n)
  */
import scala.collection.immutable.{HashMap, Map, Queue}

sealed abstract class InsertOrdMap[K, +V] extends Map[K, V] {
  def _keys: Queue[K]
  def _hashMap: Map[K, V]

  override def empty = InsertOrdMap.empty

  override def size: Int = _hashMap.size

  def iterator: Iterator[(K, V)] =
    _keys.map(k => (k, _hashMap(k))).reverse.iterator

  def get(key: K): Option[V] = _hashMap.get(key)

  def +[V2 >: V](kv: (K, V2)): InsertOrdMap[K, V2] =
    if (_hashMap.contains(kv._1))
      NonEmptyInsertOrdMap(
        _keys,
        _hashMap + kv
      )
    else
      NonEmptyInsertOrdMap(
        kv._1 +: _keys,
        _hashMap + kv
      )

  def -(k: K): InsertOrdMap[K, V] =
    NonEmptyInsertOrdMap(
      _keys.filter(k2 => k != k2),
      _hashMap - k
    )

  override def mapValues[V2](f: V => V2): InsertOrdMap[K, V2] =
    NonEmptyInsertOrdMap(_keys, _hashMap.mapValues(f))
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final case object EmptyInsertOrdMap extends InsertOrdMap[Any, Nothing] {
  override def _keys = Queue[Any]()
  override def _hashMap = HashMap[Any, Nothing]()
}

final case class NonEmptyInsertOrdMap[K, V](
    override val _keys: Queue[K],
    override val _hashMap: Map[K, V])
    extends InsertOrdMap[K, V]

object InsertOrdMap {
  def empty[K, V] = EmptyInsertOrdMap.asInstanceOf[InsertOrdMap[K, V]]

  def fromMap[K, V](m: Map[K, V]): InsertOrdMap[K, V] =
    NonEmptyInsertOrdMap(Queue(m.keys.toSeq: _*), m)

  def fromSeq[K, V](s: Seq[(K, V)]): InsertOrdMap[K, V] =
    NonEmptyInsertOrdMap(Queue(s.reverse.map(_._1): _*), HashMap(s: _*))
}
