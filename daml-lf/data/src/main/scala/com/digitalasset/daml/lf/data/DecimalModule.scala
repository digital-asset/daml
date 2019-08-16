// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.language.implicitConversions

// Our legacy Numeric with fix scale 10
abstract class DecimalModule {

  val scale: Int = 10

  // Decimal is a legacy Numeric with fix scale 10
  type Decimal <: BigDecimal

  @inline
  protected def cast(x: BigDecimal): Decimal

  private implicit def toNumeric(x: Decimal): Numeric =
    // will always succeeds
    Numeric.assertFromBigDecimal(scale, x)

  private implicit def toDecimal[Left](e: Either[Left, Numeric]): Either[Left, Decimal] =
    e.right.map(cast(_))

  val MaxValue: Decimal = cast(Numeric.assertFromString("9999999999999999999999999999.9999999999"))
  val MinValue: Decimal = cast(Numeric.assertFromString("-9999999999999999999999999999.9999999999"))

  final def fromBigDecimal(x0: BigDecimal): Either[String, Decimal] =
    Numeric.fromBigDecimal(scale, x0.bigDecimal.stripTrailingZeros)

  final def assertFromBigDecimal(x: BigDecimal): Decimal =
    assertRight(fromBigDecimal(x))

  final def add(x: Decimal, y: Decimal): Either[String, Decimal] =
    Numeric.add(x, y)

  final def div(x: Decimal, y: Decimal): Either[String, Decimal] =
    Numeric.divide(x, y)

  final def mult(x: Decimal, y: Decimal): Either[String, Decimal] =
    Numeric.multiply(x, y)

  final def sub(x: Decimal, y: Decimal): Either[String, Decimal] =
    Numeric.subtract(x, y)

  final def round(targetScale: Long, x: Decimal): Either[String, Decimal] =
    Numeric.round(targetScale, x)

  final def toLong(x: Decimal): Either[String, Long] =
    Numeric.toLong(x)

  private val hasExpectedFormat =
    """[+-]?\d{1,28}(\.\d{1,10})?""".r.pattern

  final def fromString(s: String): Either[String, Decimal] =
    if (hasExpectedFormat.matcher(s).matches())
      fromBigDecimal(BigDecimal(s))
    else
      Left(s"""Could not read Decimal string "$s"""")

  @throws[IllegalArgumentException]
  final def assertFromString(s: String): Decimal =
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

  final def fromLong(x: Long): Either[String, Decimal] =
    Numeric.fromLong(scale, x)

  final def assertFromLong(x: Long): Decimal =
    assertRight(fromLong(x))

}

object DecimalModule {

  /** You cannot overload or override any method ''names'' defined on [[BigDecimal]],
    * such as `toString` or `+`, even if the signature's different.
    * Anything else defined here will be found ''with no additional imports needed'',
    * provided that `-Xsource:2.13` is used to compile the call point.
    */
  implicit final class `Decimal methods`(private val self: Decimal) extends AnyVal {
    def decimalToString: String = Decimal toString self
  }
}
