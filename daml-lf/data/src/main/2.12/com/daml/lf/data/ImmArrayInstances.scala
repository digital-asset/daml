// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scalaz.Equal

import scala.collection.compat.immutable.ArraySeq
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.language.implicitConversions

private[data] abstract class ImmArrayInstances {
  implicit def `ImmArray canBuildFrom`[A]: CanBuildFrom[ImmArray[_], A, ImmArray[A]] =
    new ImmArrayInstances.IACanBuildFrom

  implicit def immArrayEqualInstance[A: Equal]: Equal[ImmArray[A]] =
    ScalazEqual.withNatural(Equal[A].equalIsNatural)(_ equalz _)

  def newBuilder[A]: mutable.Builder[A, ImmArray[A]] =
    ArraySeq
      .newBuilder[Any]
      .asInstanceOf[mutable.Builder[A, ArraySeq[A]]]
      .mapResult(ImmArray.fromArraySeq(_))

  def apply[A](xs: A*): ImmArray[A] = xs.to(ImmArray)

  /** Enables 2.13-style `to` calls. */
  implicit def `IS companion to CBF`[A](
      self: ImmArray.type
  ): CanBuildFrom[ImmArray[_], A, ImmArray[A]] =
    self.`ImmArray canBuildFrom`
}

private[data] object ImmArrayInstances {
  final class IACanBuildFrom[A] extends CanBuildFrom[ImmArray[_], A, ImmArray[A]] {
    override def apply(from: ImmArray[_]) = ImmArray.newBuilder[A]
    override def apply() = ImmArray.newBuilder[A]
  }
}
