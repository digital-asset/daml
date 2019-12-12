// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

package object data {

  def assertRight[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)

  val Numeric: NumericModule = new NumericModule {
    override type Numeric = java.math.BigDecimal
    @inline
    override private[data] def cast(x: java.math.BigDecimal): Numeric = x
  }
  type Numeric = Numeric.Numeric

}
