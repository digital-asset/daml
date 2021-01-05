// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.collection.generic.{CanBuildFrom, ImmutableMapFactory}
import scala.collection.immutable.{AbstractMap, Map}

abstract class AbstractInsertOrdMap[K, +V]
    extends AbstractMap[K, V]
    with Map[K, V]
    with MapKOps[K, V, InsertOrdMap[K, +*]] { this: InsertOrdMap[K, V] =>

  // we really want `abstract override` here but can't do it
  override def empty: InsertOrdMap[K, V] = InsertOrdMap.empty

  protected def removed(k: K): InsertOrdMap[K, V]

  override final def -(k: K): InsertOrdMap[K, V] = removed(k)

}

abstract class InsertOrdMapCompanion extends ImmutableMapFactory[InsertOrdMap] {
  this: InsertOrdMap.type =>

  protected type Factory[A, B] = CanBuildFrom[Coll, (A, B), InsertOrdMap[A, B]]

  protected def canBuildFrom[A, B]: Factory[A, B] =
    new MapCanBuildFrom[A, B]
}
