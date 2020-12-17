// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.collection.{IterableFactory, IterableFactoryDefaults}
import scala.collection.immutable.{AbstractSet, StrictOptimizedSetOps}
import scala.collection.mutable.ReusableBuilder

abstract class AbstractInsertOrdSet[T]
    extends AbstractSet[T]
    with IterableFactoryDefaults[T, InsertOrdSet]
    with StrictOptimizedSetOps[T, InsertOrdSet, InsertOrdSet[T]]
    with Serializable { this: InsertOrdSet[T] =>
  override final def iterableFactory: IterableFactory[InsertOrdSet] = InsertOrdSet
}

abstract class InsertOrdSetCompanion extends IterableFactory[InsertOrdSet] {
  this: InsertOrdSet.type =>
  protected type Factory[A] = Unit

  protected def canBuildFrom[A]: Factory[A] = ()

  def from[T](it: IterableOnce[T]): InsertOrdSet[T] = {
    it match {
      case s: InsertOrdSet[T] => s
      case _ => (newBuilder[T] ++= it).result()
    }
  }

  def newBuilder[T]: ReusableBuilder[T, InsertOrdSet[T]] = new InsertOrdSetBuilder[T]

  private final class InsertOrdSetBuilder[T] extends ReusableBuilder[T, InsertOrdSet[T]] {
    var m: InsertOrdSet[T] = empty;
    override def clear(): Unit = {
      m = empty;
    }
    override def result(): InsertOrdSet[T] = m
    override def addOne(elem: T): this.type = {
      m = m.incl(elem)
      this
    }
  }
}
