// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import scala.math.BigDecimal

package object data {

  val Decimal: DecimalModule = new DecimalModule {
    type T = BigDecimal
    def cast(x: BigDecimal): T = x
  }
  type Decimal = Decimal.T

  private[data] def assert[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)
}
