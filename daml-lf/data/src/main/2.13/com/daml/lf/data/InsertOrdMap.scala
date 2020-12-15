// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scalaz.{Cord, Show}

import scala.collection.{MapFactory, MapFactoryDefaults}
import scala.collection.immutable.{AbstractMap, Iterable, HashMap, Queue, StrictOptimizedMapOps}
import scala.collection.mutable.ReusableBuilder

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
    with StrictOptimizedMapOps[K, V, InsertOrdMap, InsertOrdMap[K, V]]
    with MapFactoryDefaults[K, V, InsertOrdMap, Iterable] {
  override def mapFactory: MapFactory[InsertOrdMap] = InsertOrdMap

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

  override def removed(k: K): InsertOrdMap[K, V] =
    new InsertOrdMap(keys.filter(_ != k), hashMap - k)

}

object InsertOrdMap extends MapFactory[InsertOrdMap] {

  private val Empty: InsertOrdMap[Unit, Nothing] = new InsertOrdMap(Queue.empty, HashMap.empty)

  def empty[K, V]: InsertOrdMap[K, V] = Empty.asInstanceOf[InsertOrdMap[K, V]]

  def from[K, V](it: IterableOnce[(K, V)]): InsertOrdMap[K, V] = {
    it match {
      case m: InsertOrdMap[K, V] => m
      case _ => (newBuilder[K, V] ++= it).result()
    }
  }

  def newBuilder[K, V]: ReusableBuilder[(K, V), InsertOrdMap[K, V]] =
    new InsertOrdMapBuilder[K, V]

  implicit def insertMapShow[K: Show, V: Show]: Show[InsertOrdMap[K, V]] =
    Show.show { m =>
      "InsertOrdMap[" +:
        Cord.mkCord(", ", m.toSeq.map { x =>
        Cord(implicitly[Show[K]] show x._1, "->", implicitly[Show[V]] show x._2)
      }: _*) :+ "]"
    }

  private final class InsertOrdMapBuilder[K, V]
      extends ReusableBuilder[(K, V), InsertOrdMap[K, V]] {
    var m: InsertOrdMap[K, V] = empty;
    override def clear(): Unit = {
      m = empty;
    }
    override def result(): InsertOrdMap[K, V] = m
    override def addOne(elem: (K, V)): this.type = {
      m += elem
      this
    }
  }
}
