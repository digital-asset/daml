// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import scala.math.BigDecimal

package object data {

  val Decimal: DecimalModule = new DecimalModule {
    type T = BigDecimal
    protected def cast(x: BigDecimal): T = x
  }
  type Decimal = Decimal.T

  def assertRight[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)
}
