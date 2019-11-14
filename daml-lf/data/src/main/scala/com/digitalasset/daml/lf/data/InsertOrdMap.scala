// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.collection.breakOut
import scala.collection.immutable.{HashMap, Map, Queue}

/**
  * Insert-ordered Map (like ListMap), but with efficient lookups.
  *
  * Implemented as (Queue[K], HashMap[K, V]).
  * Asymptotics:
  *  get: O(1)
  *  insert: O(1)
  *  remove: O(n)
  */
final class InsertOrdMap[Key, +Value] private (
    override val keys: Queue[Key],
    hashMap: HashMap[Key, Value]
) extends Map[Key, Value] {

  override def size: Int = hashMap.size

  override def iterator: Iterator[(Key, Value)] =
    keys.iterator.map(k => (k, hashMap(k)))

  override def get(key: Key): Option[Value] = hashMap.get(key)

  override def +[V2 >: Value](kv: (Key, V2)): InsertOrdMap[Key, V2] =
    if (hashMap.contains(kv._1))
      new InsertOrdMap(keys, hashMap + kv)
    else
      new InsertOrdMap(keys :+ kv._1, hashMap + kv)

  override def -(k: Key): InsertOrdMap[Key, Value] =
    new InsertOrdMap(keys.filter(_ != k), hashMap - k)

}

object InsertOrdMap {

  private val Empty: InsertOrdMap[Unit, Nothing] = new InsertOrdMap(Queue.empty, HashMap.empty)

  def empty[K, V]: InsertOrdMap[K, V] = Empty.asInstanceOf[InsertOrdMap[K, V]]

  def apply[K, V](entries: (K, V)*): InsertOrdMap[K, V] =
    new InsertOrdMap(entries.map(_._1)(breakOut), HashMap(entries: _*))

}
