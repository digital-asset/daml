// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scalaz.Order
import java.math.{BigDecimal, BigInteger, RoundingMode}

import scala.math.{BigDecimal => BigDec}

import scala.util.Try

/** The model of our floating point decimal numbers.
  *
  *  These are numbers of precision 38 (38 decimal digits), and variable scale (from 0 to 37 bounds
  *  included).
  */
abstract class NumericModule {

  /** Type `Numeric` represents fixed scale BigDecimals, aka Numerics. Scale of
    * Numerics can be between `0` and `maxPrecision` (bounds included). For
    * any valid scale `s` we denote `(Numeric s)` the set of Numerics of
    * scale `s`. Numerics are encoded using java BigDecimal, where the scale
    * is the scale of the Numeric.
    *
    * We use java BigDecimals instead of scala ones because:
    *  - We prefer to use here lower level methods from java
    *  - We want two numerics with different scales to be different (w.r.t. ==)
    */
  type Numeric <: BigDecimal

  /** Maximum usable precision for Numerics
    */
  val maxPrecision = 38

  val Scale: ScaleModule = new ScaleModule {
    override type Scale = Int
    override val MinValue: Scale = 0
    // We want 1 be representable at any scale, so we have to prevent (Numeric 38).
    override val MaxValue: Scale = maxPrecision - 1
    override private[NumericModule] def cast(x: Int): Scale = x
  }

  type Scale = Scale.Scale

  /** Cast the BigDecimal `x` to a `(Numeric s)` where `s` is the scale of `x`.
    */
  @inline
  private[data] def cast(x: BigDecimal): Numeric

  private val maxUnscaledValue =
    (BigInteger.TEN pow maxPrecision) subtract BigInteger.ONE

  /** Returns the largest Numeric of scale `scale`
    */
  final def maxValue(scale: Scale): Numeric =
    cast(new BigDecimal(maxUnscaledValue, scale))

  /** Returns the smallest Numeric of scale `scale`
    */
  final def minValue(scale: Scale): Numeric =
    negate(maxValue(scale))

  final def scale(numeric: Numeric): Scale =
    Scale.cast(numeric.scale)

  /** Casts `x` to a Numeric if it has a valid precision (<= `maxPrecision`). Returns an error
    * message otherwise
    *
    * ```Assumes the scale of `x` is a valid Numeric scale```
    */
  private[data] def checkForOverflow(x: BigDecimal): Either[String, Numeric] =
    Either.cond(
      x.precision <= maxPrecision,
      cast(x),
      s"Out-of-bounds (Numeric ${x.scale}) ${toString(x)}",
    )

  /** Negate the input.
    */
  final def negate(x: Numeric): Numeric =
    cast(x.negate)

  /** Adds the two Numerics. The output has the same scale as the inputs.
    * In case of overflow, returns an error message instead.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def add(x: Numeric, y: Numeric): Either[String, Numeric] = {
    assert(x.scale == y.scale)
    checkForOverflow(x add y)
  }

  /** Subtracts `y` to `x`. The output has the same scale as the inputs.
    * In case of overflow, returns an error message instead.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def subtract(x: Numeric, y: Numeric): Either[String, Numeric] = {
    assert(x.scale == y.scale)
    checkForOverflow(x subtract y)
  }

  /** Multiplies `x` by `y`. The output has the scale `scale`. If rounding must be
    * performed, the [[https://en.wikipedia.org/wiki/Rounding#Round_half_to_even> banker's rounding convention]]
    * is applied.
    * In case of overflow, returns an error message instead.
    */
  final def multiply(scale: Scale, x: Numeric, y: Numeric): Either[String, Numeric] =
    checkForOverflow((x multiply y).setScale(scale, RoundingMode.HALF_EVEN))

  /** Divides `x` by `y`. The output has the scale `scale`. If rounding must be
    * performed, the [[https://en.wikipedia.org/wiki/Rounding#Round_half_to_even> banker's rounding convention]]
    * is applied.
    * In case of overflow, returns an error message instead.
    */
  final def divide(scale: Scale, x: Numeric, y: Numeric): Either[String, Numeric] =
    checkForOverflow(x.divide(y, scale, RoundingMode.HALF_EVEN))

  /** Returns the integral part of the given decimal, in other words, rounds towards 0.
    * In case the result does not fit into a long, returns an error message instead.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def toLong(x: Numeric): Either[String, Long] =
    Try(x.setScale(0, RoundingMode.DOWN).longValueExact()).toEither.left.map(_ =>
      s"(Numeric ${x.scale}) ${toString(x)} does not fit into an Int64"
    )

  /** Rounds the `x` to the closest multiple of ``10^targetScale`` using the
    * [[https://en.wikipedia.org/wiki/Rounding#Round_half_to_even> banker's rounding convention]].
    * The output has the same scale as the input.
    * In case of overflow, returns an error message instead.
    */
  final def round(targetScale: Long, x: Numeric): Either[String, Numeric] =
    if (targetScale <= x.scale && x.scale - maxPrecision < targetScale)
      checkForOverflow(x.setScale(targetScale.toInt, RoundingMode.HALF_EVEN).setScale(x.scale))
    else
      Left(s"Bad scale $targetScale, must be between ${x.scale - maxPrecision - 1} and ${x.scale}")

  /** Returns -1, 0, or 1 as `x` is numerically less than, equal to, or greater than `y`.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def compareTo(x: Numeric, y: Numeric): Int = {
    assert(x.scale == y.scale)
    x compareTo y
  }

  /** Converts the java BigDecimal `x` to a `(Numeric scale)``.
    * In case scale is not a valid Numeric scale or `x` cannot be represented as a
    * `(Numeric scale)` without loss of precision, returns an error message instead.
    */
  final def fromBigDecimal(scale: Scale, x: BigDecimal): Either[String, Numeric] =
    if (!(x.stripTrailingZeros.scale <= scale))
      Left(s"Cannot represent ${toString(x)} as (Numeric $scale) without loss of precision")
    else
      checkForOverflow(x.setScale(scale, RoundingMode.UNNECESSARY))

  /** Like `fromBigDecimal(Int, BigDecimal)` but with a scala BigDecimal.
    */
  final def fromBigDecimal(scale: Scale, x: BigDec): Either[String, Numeric] =
    fromBigDecimal(scale, x.bigDecimal)

  /** Converts the Long `x` to a `(Numeric scale)``.
    * In case scale is not a valid Numeric scale or `x` cannot be represented as a
    * `(Numeric scale)`, returns an error message instead.
    */
  final def fromLong(scale: Scale, x: Long): Either[String, Numeric] =
    fromBigDecimal(scale, BigDecimal.valueOf(x))

  /** Like `fromBigDecimal` but throws an exception instead of returning e message in case of error.
    */
  @throws[IllegalArgumentException]
  final def assertFromBigDecimal(scale: Scale, x: BigDecimal): Numeric =
    assertRight(fromBigDecimal(scale, x))

  /** Like `fromBigDecimal` but throws an exception instead of returning e message in case of error.
    */
  @throws[IllegalArgumentException]
  final def assertFromBigDecimal(scale: Scale, x: BigDec): Numeric =
    assertRight(fromBigDecimal(scale, x))

  /** Returns a canonical decimal string representation of `x`. The output string consists of (in
    * left-to-right order):
    * - An optional negative sign ("-") to indicate if `x` is strictly negative.
    * - The integral part of `x` without leading '0' if the integral part of `x` is not equal to 0, "0" otherwise.
    * - the decimal point (".") to separate the integral part from the decimal part,
    * - the decimal part of `x` with '0' padded to match the scale. The number of decimal digits must be the same as the scale.
    */
  final def toString(x: BigDecimal): String = {
    val s = x.toPlainString
    if (x.scale <= 0) s + "." else s
  }

  private val validScaledFormat =
    """-?([1-9]\d*|0)\.(\d*)""".r

  /** Given a string representation of a decimal returns the corresponding Numeric, where the number of
    * digits to the right of the decimal point indicates the scale.
    * If the input does not match
    *   `-?([1-9]\d*|0).(\d*)`
    * or if the result of the conversion cannot be mapped into a numeric without loss of precision
    * returns an error message instead.
    */
  def fromString(s: String): Either[String, Numeric] = {
    def errMsg = s"""Could not read Numeric string "$s""""
    s match {
      case validScaledFormat(intPart, decPart) =>
        val scale = decPart.length
        val precision = if (intPart == "0") scale else intPart.length + scale
        Either.cond(
          precision <= maxPrecision && scale <= Scale.MaxValue,
          cast(new BigDecimal(s).setScale(scale)),
          errMsg,
        )
      case _ =>
        Left(errMsg)
    }
  }

  /** Like `fromString`, but throws an exception instead of returning a message in case of error.
    */
  @throws[IllegalArgumentException]
  def assertFromString(s: String): Numeric =
    assertRight(fromString(s))

  /** Given a string representation of a decimal and a scale s returns the corresponding `Numeric s`.
    * If the input does not match (where i = 38-s)
    *   `-?([1-9]\d{1,i}|0).([1-9]{s})`
    * or if the result of the conversion cannot be mapped into a numeric of scale `s` without loss of precision
    * returns an error message instead.
    */
  def fromString(s: Scale, str: String): Either[String, Numeric] = for {
    n <- fromString(str)
    _ <- Either.cond(n.scale() == s, (), s"""Could not read Numeric $s string "$str"""")
  } yield n

  @throws[IllegalArgumentException]
  def assertFromString(scale: Scale, s: String) =
    assertRight(fromString(scale, s))

  /** Convert a BigDecimal to the Numeric with the smallest possible scale able to represent the
    * former without loss of precision. Returns an error if such Numeric does not exists.
    *
    * Use this function to convert BigDecimal with unknown scale.
    */
  final def fromUnscaledBigDecimal(x: BigDecimal): Either[String, Numeric] =
    for {
      s <- Scale.fromInt(x.stripTrailingZeros().scale max 0)
      n <- fromBigDecimal(s, x)
    } yield n

  /** Like the previous function but with scala BigDecimal
    */
  final def fromUnscaledBigDecimal(x: BigDec): Either[String, Numeric] =
    fromUnscaledBigDecimal(x.bigDecimal)

  /** Like fromUnscaledBigDecimal, but throws an exception instead of returning a message in case of error.
    */
  @throws[IllegalArgumentException]
  final def assertFromUnscaledBigDecimal(x: BigDec): Numeric =
    assertRight(fromUnscaledBigDecimal(x))

  final def toUnscaledString(x: BigDecimal): String = {
    // Strip the trailing zeros (which BigDecimal keeps if the string
    // it was created from had them), and use the plain notation rather
    // than scientific notation.
    //
    // Moreover, add a single trailing zero if we have no decimal part.
    // this mimicks the behavior of `formatScientific Fixed Nothing` which
    // we've been using in Haskell to render Decimals
    // http://hackage.haskell.org/package/scientific-0.3.6.2/docs/Data-Scientific.html#v:formatScientific
    val s = x.stripTrailingZeros.toPlainString
    if (s.contains(".")) s else s + ".0"
  }

  /** Checks that a BigDecimal falls between `minValue(scale)` and
    * `maxValue(scale)`, and round the number according to `scale`. Note
    * that it does _not_ fail if the number contains data beyond `scale`.
    */
  def checkWithinBoundsAndRound(scale: Scale, x: BigDecimal): Either[String, Numeric] =
    Either.cond(
      x.precision - x.scale <= maxPrecision - scale,
      cast(x.setScale(scale, RoundingMode.HALF_EVEN)),
      s"out-of-bounds (Numeric $scale) $x",
    )

  /** Like the previous function but with scala BigDecimals instead of java ones.
    */
  def checkWithinBoundsAndRound(scale: Scale, x: BigDec): Either[String, Numeric] =
    checkWithinBoundsAndRound(scale, x.bigDecimal)

  sealed abstract class ScaleModule {
    type Scale <: Int

    val MinValue: Scale

    val MaxValue: Scale

    lazy val values: Vector[Scale] = Vector.range(MinValue, MaxValue + 1).map(cast)

    /** Cast the Int `x` to a `Scale`.
      */
    @inline
    private[NumericModule] def cast(x: Int): Scale

    /** Converts Int to an Scale.
      * Returns an error if `scale` is not between `minScale` and `maxScale`.
      */
    final def fromInt(scale: Int): Either[String, Scale] =
      Either.cond(
        MinValue <= scale && scale <= MaxValue,
        cast(scale),
        s"Bad scale $scale, must be between 0 and $MaxValue",
      )

    /** Like `fromInt` but throws an exception instead of returning e message in case of error.
      */
    final def assertFromInt(scale: Int): Scale =
      assertRight(fromInt(scale))

    /** Converts Long to an Scale.
      * Returns an error if `scale` is not between `minScale` and `maxScale`.
      */
    final def fromLong(scale: Long): Either[String, Scale] =
      Either.cond(
        MinValue <= scale && scale <= MaxValue,
        cast(scale.toInt),
        s"Bad scale $scale, must be between 0 and $MaxValue",
      )

    /** Like `fromLong` but throws an exception instead of returning e message in case of error.
      */
    final def assertFromLong(scale: Long): Scale =
      assertRight(fromLong(scale))

  }

}

object NumericModule {

  implicit final class `Numeric methods`(private val self: Numeric) extends AnyVal {
    def toUnscaledString: String = Numeric toUnscaledString self
    def toScaledString: String = Numeric toString self
  }

  implicit def `Numeric Order`: Order[Numeric] =
    Order.fromScalaOrdering[BigDecimal].contramap(identity)

}
