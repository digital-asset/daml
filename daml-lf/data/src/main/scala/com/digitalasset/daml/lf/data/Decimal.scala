// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.math.BigDecimal

import com.digitalasset.daml.lf.data

import scala.math.{BigDecimal => BigDec}

// Our legacy Numerics are fix scale 10, aka Decimals
// This object provides some legacy utility functions for Decimal
object Decimal {

  val scale: Int = 10

  val MaxValue: Numeric = Numeric.assertFromString("9999999999999999999999999999.9999999999")
  val MinValue: Numeric = Numeric.assertFromString("-9999999999999999999999999999.9999999999")

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
