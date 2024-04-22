// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import com.daml.lf.data
import java.math.BigDecimal

import scala.math.{BigDecimal => BigDec}

// Our legacy Numerics are fix scale 10, aka Decimals
// This object provides some legacy utility functions for Decimal
@deprecated("Use Numeric instead", since = "3.0.0")
object Decimal {

  @deprecated("Use Numeric.Scale.assertFromInt(10)", since = "3.0.0")
  val scale: Numeric.Scale = Numeric.Scale.assertFromInt(10)

  @deprecated("Use Numeric.Scale.assertFromInt(10).MaxValue", since = "3.0.0")
  def MaxValue: Numeric = Numeric.maxValue(scale)
  @deprecated("Use Numeric.Scale.assertFromInt(10).MinValue", since = "3.0.0")
  def MinValue: Numeric = Numeric.minValue(scale)

  @deprecated("Use Numeric.fromBigDecimal(Scale.assertFromInt(10), _)", since = "3.0.0")
  def fromBigDecimal(x: BigDec): Either[String, data.Numeric.Numeric] =
    Numeric.fromBigDecimal(scale, x)

  private val hasExpectedFormat =
    """[+-]?\d{1,28}(\.\d{1,10})?""".r.pattern

  @deprecated(
    """Use Numeric.fromString(Scale.assertFromInt(10), _). Note this alternative is stricter as it requires the string to match "`-?[1-9]\d{1,28}.([1-9]{10})`"""",
    since = "3.0.0",
  )
  final def fromString(s: String): Either[String, Numeric] =
    if (hasExpectedFormat.matcher(s).matches())
      Numeric.fromBigDecimal(scale, new BigDecimal(s))
    else
      Left(s"""Could not read Decimal string "$s"""")

  @deprecated("Use Numeric.fromString(Scale.assertFromInt(10), _)", since = "3.0.0")
  final def assertFromString(s: String): Numeric =
    assertRight(fromString(s))

}
