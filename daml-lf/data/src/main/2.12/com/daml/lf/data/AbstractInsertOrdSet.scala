// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.collection.immutable.Set
import scala.collection.{SetLike, AbstractSet}
import scala.collection.generic.{
  ImmutableSetFactory,
  GenericCompanion,
  CanBuildFrom,
  GenericSetTemplate
}

abstract class AbstractInsertOrdSet[T]
    extends AbstractSet[T]
    with Set[T]
    with SetLike[T, InsertOrdSet[T]]
    with GenericSetTemplate[T, InsertOrdSet]
    with Serializable { this: InsertOrdSet[T] =>
  override final def companion: GenericCompanion[InsertOrdSet] = InsertOrdSet

  protected def incl(elem: T): InsertOrdSet[T]
  protected def excl(elem: T): InsertOrdSet[T]

  override final def +(elem: T): InsertOrdSet[T] =
    incl(elem)
  override final def -(elem: T): InsertOrdSet[T] =
    excl(elem)

}

abstract class InsertOrdSetCompanion extends ImmutableSetFactory[InsertOrdSet] {
  this: InsertOrdSet.type =>
  def emptyInstance: InsertOrdSet[Any] = empty[Any]

  protected type Factory[A] = CanBuildFrom[Coll, A, InsertOrdSet[A]]

  protected def canBuildFrom[A]: Factory[A] =
    setCanBuildFrom[A]

}
