// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import java.math.BigDecimal

import scala.math.{BigDecimal => BigDec}

// Our legacy Numerics are fix scale 10, aka Decimals
// This object provides some legacy utility functions for Decimal
object Decimal {

  val scale: Numeric.Scale = Numeric.Scale.assertFromInt(10)

  def MaxValue: Numeric = Numeric.maxValue(scale)
  def MinValue: Numeric = Numeric.minValue(scale)

  def fromBigDecimal(x: BigDec): Either[String, data.Numeric.Numeric] =
    Numeric.fromBigDecimal(scale, x)

  private val hasExpectedFormat =
    """[+-]?\d{1,28}(\.\d{1,10})?""".r.pattern

  final def fromString(s: String): Either[String, Numeric] =
    if (hasExpectedFormat.matcher(s).matches())
      Numeric.fromBigDecimal(scale, new BigDecimal(s))
    else
      Left(s"""Could not read Decimal string "$s"""")

  final def assertFromString(s: String): Numeric =
    assertRight(fromString(s))

}
