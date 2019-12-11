// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scalaz.{Cord, Show}

import scala.collection.generic.{CanBuildFrom, ImmutableMapFactory}
import scala.collection.immutable.{AbstractMap, HashMap, Map, Queue}

/**
  * Insert-ordered Map (like ListMap), but with efficient lookups.
  *
  * Implemented as (Queue[K], HashMap[K, V]).
  * Asymptotics:
  *  get: O(1)
  *  insert: O(1)
  *  remove: O(n)
  *  in order traversal: O(n)
  */
final class InsertOrdMap[K, +V] private (
    override val keys: Queue[K],
    hashMap: HashMap[K, V]
) extends AbstractMap[K, V]
    with Map[K, V]
    with MapKOps[K, V, InsertOrdMap[K, +?]] {

  override def empty: InsertOrdMap[K, V] = InsertOrdMap.empty[K, V]

  override def size: Int = hashMap.size

  override def iterator: Iterator[(K, V)] =
    keys.iterator.map(k => (k, hashMap(k)))

  override def get(key: K): Option[V] = hashMap.get(key)

  override def updated[V1 >: V](key: K, value: V1): InsertOrdMap[K, V1] =
    new InsertOrdMap(if (hashMap.contains(key)) keys else keys :+ key, hashMap.updated(key, value))

  override def +[V1 >: V](kv: (K, V1)): InsertOrdMap[K, V1] = {
    val (key, value) = kv
    updated(key, value)
  }

  override def -(k: K): InsertOrdMap[K, V] =
    new InsertOrdMap(keys.filter(_ != k), hashMap - k)

}

object InsertOrdMap extends ImmutableMapFactory[InsertOrdMap] {

  private val Empty: InsertOrdMap[Unit, Nothing] = new InsertOrdMap(Queue.empty, HashMap.empty)

  def empty[K, V]: InsertOrdMap[K, V] = Empty.asInstanceOf[InsertOrdMap[K, V]]

  implicit def canBuildFrom[A, B]: CanBuildFrom[Coll, (A, B), InsertOrdMap[A, B]] =
    new MapCanBuildFrom[A, B]

  implicit def insertMapShow[K: Show, V: Show]: Show[InsertOrdMap[K, V]] =
    Show.show { m =>
      "InsertOrdMap[" +:
        Cord.mkCord(", ", m.toSeq.map { x =>
        Cord(implicitly[Show[K]] show x._1, "->", implicitly[Show[V]] show x._2)
      }: _*) :+ "]"
    }

}
