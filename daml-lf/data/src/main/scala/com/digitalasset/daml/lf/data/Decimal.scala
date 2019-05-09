// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.math.MathContext

import scala.math.BigDecimal

/** The model of our floating point decimal numbers.
  *
  *  These are numbers of precision 38 (38 decimal digits), and scale 10 (10 digits after the comma)
  */
object Decimal {

  sealed abstract class AbstractDecimalModule {
    type T <: BigDecimal
    private[Decimal] def cast(t: BigDecimal): T
  }

  val DecimalModule: AbstractDecimalModule = new AbstractDecimalModule {
    type T = BigDecimal
    private[Decimal] def cast(t: BigDecimal): T = t
  }

  import DecimalModule._

  val scale: Int = 10
  val context: MathContext = new MathContext(38, java.math.RoundingMode.HALF_EVEN)

  private def unlimitedBigDecimal(s: String): Decimal =
    cast(BigDecimal.decimal(new java.math.BigDecimal(s), MathContext.UNLIMITED))
  private def unlimitedBigDecimal(x: Long): Decimal =
    cast(BigDecimal(new java.math.BigDecimal(x, MathContext.UNLIMITED)))

  // we use these to compare only, therefore set the precision to unlimited to make sure
  // we can compare every number we're given
  val max: T = unlimitedBigDecimal("9999999999999999999999999999.9999999999")

  val min: T = unlimitedBigDecimal("-9999999999999999999999999999.9999999999")

  /** Checks that a `Decimal` falls between `min` and `max`, and
    * round the number according to `scale`. Note that it does _not_
    * fail if the number contains data beyond `scale`.
    */
  private def checkWithinBoundsAndRound(x0: BigDecimal): Either[String, T] = {
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
  def fromBigDecimal(x0: BigDecimal): Either[String, T] =
    for {
      x1 <- checkWithinBoundsAndRound(x0)
      // if we've lost any data at all, it means that we weren't within the
      // scale.
      x2 <- Either.cond(x0 == x1, x1, s"out-of-bounds Decimal $x0")
    } yield x2

  def assertFromBigDecimal(x: BigDecimal): T =
    assert(fromBigDecimal(x))

  def add(x: T, y: T): Either[String, T] = checkWithinBoundsAndRound(x + y)
  def div(x: T, y: T): Either[String, T] = checkWithinBoundsAndRound(x / y)
  def mult(x: T, y: T): Either[String, T] = checkWithinBoundsAndRound(x * y)
  def sub(x: T, y: T): Either[String, T] = checkWithinBoundsAndRound(x - y)

  def round(newScale: Long, x0: T): Either[String, T] =
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

  def fromString(s: String): Either[String, T] =
    if (hasExpectedFormat.matcher(s).matches())
      fromBigDecimal(unlimitedBigDecimal(s))
    else
      Left(s"""Could not read Decimal string "$s"""")

  def assertFromString(s: String): T =
    assert(fromString(s))

  def toString(d: T): String = {
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

  def toUtf8String(d: Decimal) =
    Utf8String(toString(d))

  def fromLong(x: Long): Decimal =
    cast(BigDecimal(new java.math.BigDecimal(x, context)).setScale(scale))

  private val toLongLowerBound = unlimitedBigDecimal(Long.MinValue) - 1
  private val toLongUpperBound = unlimitedBigDecimal(Long.MaxValue) + 1

  def toLong(x: T): Either[String, Long] = {
    if (toLongLowerBound < x && x < toLongUpperBound)
      Right(x.longValue)
    else
      Left(s"Decimal $x does not fit into an Int64")
  }

  private def assert[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)

}
