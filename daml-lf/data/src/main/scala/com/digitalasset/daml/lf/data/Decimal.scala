// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

/** Legacy utilities for Decimals.
  */
object Decimal {

  val scale: Int = 10

  val MaxValue: BigDecimal = BigDecimal("9999999999999999999999999999.9999999999")
  val MinValue: BigDecimal = -MaxValue

  final def fromBigDecimal(x0: BigDecimal): Either[String, BigDecimal] =
    Numeric.fromBigDecimal(10, x0)

  final def assertFromBigDecimal(x: BigDecimal): BigDecimal =
    assertRight(fromBigDecimal(x))

  private val hasExpectedFormat =
    """[+-]?\d{1,28}(\.\d{1,10})?""".r.pattern

  final def fromString(s: String): Either[String, BigDecimal] =
    if (hasExpectedFormat.matcher(s).matches())
      fromBigDecimal(BigDecimal(s))
    else
      Left(s"""Could not read Decimal string "$s"""")

  @throws[IllegalArgumentException]
  final def assertFromString(s: String): BigDecimal =
    assertRight(fromString(s))

  final def toString(d: BigDecimal): String = {
    // Strip the trailing zeros (which BigDecimal keeps if the string
    // it was created from had them), and use the plain notation rather
    // than scientific notation.
    //
    // Moreover, add a single trailing zero if we have no decimal part.
    // this mimicks the behavior of `formatScientific Fixed Nothing` which
    // we've been using in Haskell to render Decimals
    // http://hackage.haskell.org/package/scientific-0.3.6.2/docs/Data-Scientific.html#v:formatScientific
    val s = d.bigDecimal.stripTrailingZeros.toPlainString
    if (s.contains(".")) s else s + ".0"
  }

}
