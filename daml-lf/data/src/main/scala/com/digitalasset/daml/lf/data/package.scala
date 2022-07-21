// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

package object data {

  @throws[IllegalArgumentException]
  def assertRight[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)

  val Numeric: NumericModule = new NumericModule {
    override type Numeric = java.math.BigDecimal
    @inline
    override private[data] def cast(x: java.math.BigDecimal): Numeric = x
  }
  type Numeric = Numeric.Numeric

  type Relation[A, B] = Map[A, Set[B]]

}
