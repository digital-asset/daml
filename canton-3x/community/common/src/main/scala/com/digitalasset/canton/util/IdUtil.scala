// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.{Id, Semigroup}

/** Contains instances for the `Id` functor. */
object IdUtil {

  /** Right-biased semigroup instance for any type:
    * `combine` keeps the value on the right and discards the one on the left
    */
  implicit def catsSemigroupForIdRightBias[A]: Semigroup[Id[A]] = new Semigroup[Id[A]] {
    override def combine(x: A, y: A): A = y

    protected[this] override def repeatedCombineN(a: A, n: Int): A = a
  }

  /** Left-biased semigroup instance for any type:
    * `combine` keeps the value on the left and discards the one on the right
    */
  implicit def catsSemigroupForIdLeftBias[A]: Semigroup[Id[A]] = new Semigroup[Id[A]] {
    override def combine(x: A, y: A): A = x

    protected[this] override def repeatedCombineN(a: A, n: Int): A = a
  }
}
