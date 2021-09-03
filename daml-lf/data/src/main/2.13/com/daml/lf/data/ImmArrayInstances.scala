// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scalaz.Equal

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.Builder

abstract class ImmArrayInstances extends scala.collection.IterableFactory[ImmArray] {
  implicit def immArrayEqualInstance[A: Equal]: Equal[ImmArray[A]] =
    ScalazEqual.withNatural(Equal[A].equalIsNatural)(_ equalz _)

  override def from[A](it: IterableOnce[A]): ImmArray[A] = {
    it match {
      case arraySeq: ImmArray.ImmArraySeq[A] =>
        arraySeq.toImmArray
      case otherwise =>
        val builder = newBuilder[A]
        builder.sizeHint(otherwise)
        builder.addAll(otherwise)
        builder.result()
    }
  }

  override def newBuilder[A]: Builder[A, ImmArray[A]] =
    ArraySeq
      .newBuilder[Any]
      .asInstanceOf[Builder[A, ArraySeq[A]]]
      .mapResult(ImmArray.fromArraySeq(_))
}
