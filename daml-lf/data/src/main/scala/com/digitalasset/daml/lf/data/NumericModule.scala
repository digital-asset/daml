// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.math.BigDecimal
import scala.math.{BigDecimal => BigDec}
import BigDecimal.{ROUND_DOWN, ROUND_HALF_EVEN, ROUND_UNNECESSARY}

import scala.util.Try

abstract class NumericModule {

  /**
    * Type `Numeric` represents fixed scale BigDecimals, aka Numerics. Scale of
    * Numerics can be between `0` and `maxPrecision` (bounds included). For
    * any valid scale `s` we denote `(Numeric s)` the set of Numerics of
    * scale `s`. Numerics are encoded using java BigDecimal, where the scale
    * is the scale of the Numeric.
    */
  type Numeric <: BigDecimal

  /**
    * Maximum usable precision for Numerics
    */
  val maxPrecision = 38

  /**
    * Cast the BigDecimal `x` to a `(Numeric s)` where `s` is the scale of `x`.
    */
  @inline
  private[data] def cast(x: BigDecimal): Numeric

  /**
    * Casts `x` to a Numeric if it has a valid precision (<= `maxPrecision`). Returns an error
    * message otherwise
    *
    * ```Assumes the scale of `x` is a valid Numeric scale```
    */
  private[data] def checkForOverflow(x: BigDecimal): Either[String, Numeric] =
    Either.cond(
      x.precision() <= maxPrecision,
      cast(x),
      s"Out-of-bounds (Numeric ${x.scale}) $x"
    )

  /**
    * Adds the two Numerics. The output has the same scale than the inputs.
    * In case of overflow, returns an error message instead.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def add(x: Numeric, y: Numeric): Either[String, Numeric] = {
    assert(x.scale == y.scale)
    checkForOverflow(x add y)
  }

  /**
    * Subtracts `y` to `x`. The output has the same scale than the inputs.
    * In case of overflow, returns an error message instead.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def subtract(x: Numeric, y: Numeric): Either[String, Numeric] = {
    assert(x.scale == y.scale)
    checkForOverflow(x subtract y)
  }

  /**
    * Multiplies `x` by `y`. The output has the same scale than the inputs. If rounding must be
    * performed, the [[https://en.wikipedia.org/wiki/Rounding#Round_half_to_even> banker's rounding convention]]
    * is applied.
    * In case of overflow, returns an error message instead.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def multiply(x: Numeric, y: Numeric): Either[String, Numeric] = {
    assert(x.scale == y.scale)
    checkForOverflow((x multiply y).setScale(x.scale(), ROUND_HALF_EVEN))
  }

  /**
    * Divides `x` by `y`. The output has the same scale than the inputs. If rounding must be
    * performed, the [[https://en.wikipedia.org/wiki/Rounding#Round_half_to_even> banker's rounding convention]]
    * is applied.
    * In case of overflow, returns an error message instead.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def divide(x: Numeric, y: Numeric): Either[String, Numeric] = {
    assert(x.scale == y.scale)
    checkForOverflow(x.divide(y, x.scale(), ROUND_HALF_EVEN))
  }

  /**
    * Returns the integral part of the given decimal, in other words, rounds towards 0.
    * In case of the result does not fit a lonf, returns an error message instead.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def toLong(x: Numeric): Either[String, Long] =
    Try(x.setScale(0, ROUND_DOWN).longValueExact()).toEither.left.map(
      _ => s"(Numeric ${x.scale()}) $x does not fit into an Int64",
    )

  /**
    * Rounds the `x` to the closest multiple of ``10⁻ˢᶜᵃˡᵉ`` using the
    * [[https://en.wikipedia.org/wiki/Rounding#Round_half_to_even> banker's rounding convention]].
    * The output has the same scale than the inputs.
    * In case of overflow, returns an error message instead.
    */
  final def round(newScale: Long, x: Numeric): Either[String, Numeric] =
    if (newScale <= x.scale && x.scale - maxPrecision < newScale)
      checkForOverflow(x.setScale(newScale.toInt, ROUND_HALF_EVEN).setScale(x.scale))
    else
      Left(s"Bad scale $newScale, must be between ${x.scale - maxPrecision - 1} and ${x.scale}")

  /**
    * Returns -1, 0, or 1 as `x` is numerically less than, equal to, or greater than `y`.
    *
    * ```Requires the scale of `x` and `y` are the same.```
    */
  final def compare(x: Numeric, y: Numeric): Int = {
    assert(x.scale == y.scale)
    x compareTo y
  }

  /**
    * Converts the java BigDecimal `x` to a `(Numeric scale)``.
    * In case scale is a not a valid Numeric scale or `x` cannot be represented as a
    * `(Numeric scale)` without or loss of precision, returns an error message instead.
    */
  final def fromBigDecimal(scale: Int, x: BigDecimal): Either[String, Numeric] =
    if (!(0 <= scale && scale <= maxPrecision))
      Left(s"Bad scale $scale, must be between 0 and $maxPrecision")
    else if (!(x.scale() <= scale))
      Left(s"Cannot represent $x as (Numeric $scale) without lost of precision")
    else
      checkForOverflow(x.setScale(scale, ROUND_UNNECESSARY))

  /**
    * Like `fromBigDec` but with a scala BigDecimal.
    */
  final def fromBigDecimal(scale: Int, x: BigDec): Either[String, Numeric] =
    fromBigDecimal(scale, x.bigDecimal)

  /**
    * Like `fromBigDecimal` but throws an exception instead of returning e message in case of error.
    */
  @throws[IllegalArgumentException]
  final def assertFromBigDecimal(scale: Int, x: BigDec): Numeric =
    assertRight(fromBigDecimal(scale, x))

  private val validFormat = """-?([1-9]\d*|0).(\d*)""".r

  /*
   * Returns a canonical decimal string representation of `x`. The output string consists of (in
   * left-to-right order):
   * - An optional negative sign ("-") to indicate if `x` is strictly negative.
   * - The integral part of `x` if `x` without leading '0' if 'x' is not null, "0" otherwise.
   * - the decimal point (".") to separated the integral part from the decimal part,
   * - the decimal part of `x` with possibly trailing '0', the number of digits indicating the scale
   * of `x`.
   */
  final def toString(x: Numeric): String = {
    val s = x.toPlainString
    if (x.scale == 0) s + "." else s
  }

  /*
   * Given a string representation of a decimal returns the correspond Numeric, where the number of
   * digit to the right of te decimal point indicates the scale.
   * If the input does not match
   *   `-?([1-9]\d*|0).(\d*)`
   * or if the result of the conversion cannot be mapped into a numerics without loss of precision
   * returns an error message instead.
   */
  def fromString(s: String): Either[String, Numeric] = {
    def errMsg = s"""Could not read Numeric string "$s""""
    s match {
      case validFormat(intPart, decPart) =>
        val scale = decPart.length
        val precision = if (intPart == "0") scale else intPart.length + scale
        Either.cond(precision <= maxPrecision, cast(new BigDecimal(s).setScale(scale)), errMsg)
      case _ =>
        Left(errMsg)
    }
  }

  /*
   * Like `fromString`, but throws an exception instead of an error message.
   */
  @throws[IllegalArgumentException]
  def assertFromString(s: String): Numeric =
    assertRight(fromString(s))

}
