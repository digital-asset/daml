// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.math.MathContext

import scala.math.BigDecimal

/** The model of our floating point decimal numbers.
  *
  *  These are numbers of precision 38 (38 decimal digits), and scale 10 (10 digits after the comma)
  */
abstract class DecimalModule {

  type T <: BigDecimal

  protected def cast(x: BigDecimal): T

  val scale: Int = 10
  val context: MathContext = new MathContext(38, java.math.RoundingMode.HALF_EVEN)

  private def unlimitedBigDecimal(s: String): T =
    cast(BigDecimal.decimal(new java.math.BigDecimal(s), MathContext.UNLIMITED))

  private def unlimitedBigDecimal(x: Long): T =
    cast(BigDecimal(new java.math.BigDecimal(x, MathContext.UNLIMITED)))

  // we use these to compare only, therefore set the precision to unlimited to make sure
  // we can compare every number we're given.
  private val max: BigDecimal = unlimitedBigDecimal("9999999999999999999999999999.9999999999")
  private val min: BigDecimal = unlimitedBigDecimal("-9999999999999999999999999999.9999999999")

  val MaxValue: T = assertFromBigDecimal(max)
  val MinValue: T = assertFromBigDecimal(min)

  /** Checks that a `T` falls between `min` and `max`, and
    * round the number according to `scale`. Note that it does _not_
    * fail if the number contains data beyond `scale`.
    */
  private[lf] def checkWithinBoundsAndRound(x0: BigDecimal): Either[String, T] = {
    if (x0 > max || x0 < min) {
      Left(s"out-of-bounds Decimal $x0")
    } else {
      val x1 = new BigDecimal(x0.bigDecimal, context)
      val x2 = x1.setScale(scale, BigDecimal.RoundingMode.HALF_EVEN)
      Right(cast(x2))
    }
  }

  /** Like `checkWithinBoundsAndRound`, but _fails_ if the given number contains
    * any data beyond `scale`.
    */
  final def fromBigDecimal(x0: BigDecimal): Either[String, T] =
    for {
      x1 <- checkWithinBoundsAndRound(x0)
      // if we've lost any data at all, it means that we weren't within the
      // scale.
      x2 <- Either.cond(x0 == x1, x1, s"out-of-bounds Decimal $x0")
    } yield x2

  final def assertFromBigDecimal(x: BigDecimal): T =
    assert(fromBigDecimal(x))

  final def add(x: T, y: T): Either[String, T] = checkWithinBoundsAndRound(x + y)

  final def div(x: T, y: T): Either[String, T] = checkWithinBoundsAndRound(x / y)

  final def mult(x: T, y: T): Either[String, T] = checkWithinBoundsAndRound(x * y)

  final def sub(x: T, y: T): Either[String, T] = checkWithinBoundsAndRound(x - y)

  final def round(newScale: Long, x0: T): Either[String, T] =
    // check to make sure the rounding mode is OK
    checkWithinBoundsAndRound(x0).flatMap(
      x =>
        if (newScale > scale || newScale < -27)
          Left(s"Bad scale $newScale, must be between -27 and $scale")
        else
          // we know toIntExact won't crash because we checked the scale above
          // we set the scale again to make sure that every Decimal has scale 10, which
          // affects equality
          Right(
            cast(
              x.setScale(Math.toIntExact(newScale), BigDecimal.RoundingMode.HALF_EVEN)
                .setScale(scale))))

  private val hasExpectedFormat =
    """[+-]?\d{1,28}(\.\d{1,10})?""".r.pattern

  final def fromString(s: String): Either[String, T] =
    if (hasExpectedFormat.matcher(s).matches())
      fromBigDecimal(unlimitedBigDecimal(s))
    else
      Left(s"""Could not read Decimal string "$s"""")

  @throws[IllegalArgumentException]
  final def assertFromString(s: String): T =
    assert(fromString(s))

  final def toString(d: T): String = {
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

  final def fromLong(x: Long): T =
    cast(BigDecimal(new java.math.BigDecimal(x, context)).setScale(scale))

  private val toLongLowerBound = unlimitedBigDecimal(Long.MinValue) - 1
  private val toLongUpperBound = unlimitedBigDecimal(Long.MaxValue) + 1

  final def toLong(x: T): Either[String, Long] =
    Either.cond(
      toLongLowerBound < x && x < toLongUpperBound,
      x.longValue,
      s"Decimal $x does not fit into an Int64")

}

object DecimalModule {

  /** You cannot overload or override any method ''names'' defined on [[BigDecimal]],
    * such as `toString` or `+`, even if the signature's different.
    * Anything else defined here will be found ''with no additional imports needed'',
    * provided that `-Xsource:2.13` is used to compile the call point.
    */
  implicit final class `Decimal methods`(private val self: Decimal) extends AnyVal {
    def decimalToString: String = Decimal toString self
    def decimalToLong: Either[String, Long] = Decimal toLong self
  }
}
