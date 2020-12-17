// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.collection.{MapFactory, MapFactoryDefaults}
import scala.collection.immutable.{AbstractMap, Iterable, StrictOptimizedMapOps}
import scala.collection.mutable.ReusableBuilder

abstract class AbstractInsertOrdMap[K, +V]
    extends AbstractMap[K, V]
    with StrictOptimizedMapOps[K, V, InsertOrdMap, InsertOrdMap[K, V]]
    with MapFactoryDefaults[K, V, InsertOrdMap, Iterable] { this: InsertOrdMap[K, V] =>
  override final def mapFactory: MapFactory[InsertOrdMap] = InsertOrdMap

}

abstract class InsertOrdMapCompanion extends MapFactory[InsertOrdMap] { this: InsertOrdMap.type =>

  protected type Factory[A, B] = Unit

  protected def canBuildFrom[A, B]: Factory[A, B] = ()

  def from[K, V](it: IterableOnce[(K, V)]): InsertOrdMap[K, V] = {
    it match {
      case m: InsertOrdMap[K, V] => m
      case _ => (newBuilder[K, V] ++= it).result()
    }
  }

  def newBuilder[K, V]: ReusableBuilder[(K, V), InsertOrdMap[K, V]] =
    new InsertOrdMapBuilder[K, V]

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
