// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.math.BigDecimal.{ROUND_DOWN, ROUND_HALF_EVEN}

import scala.math.BigDecimal
import scala.util.Try

object Numeric {

  val legacyDecimalScale = 10
  val maxPrecision = 38

  // double check that the BigDecimal has the expected scale and precision
  private def assertNumeric(scale: Int, x: BigDecimal) = {
    assert(x.scale <= scale)
    assert(x.precision - x.scale <= maxPrecision - scale)
  }

  // assume that x.scale <= scale
  private def checkForOverflow(scale: Int, x: BigDecimal): Either[String, BigDecimal] =
    Either.cond(
      x.precision - x.scale <= maxPrecision - scale,
      x,
      s"Out-of-bounds (Numeric $scale) ${toString(x)}"
    )

  private def checkScale(scale: Int, x: BigDecimal): Either[String, BigDecimal] =
    Either.cond(
      x.scale <= scale,
      x,
      s"Cannot represent ${toString(x)} as (Numeric $scale) without lost of precision"
    )

  def add(scale: Int, x: BigDecimal, y: BigDecimal): Either[String, BigDecimal] = {
    assertNumeric(scale, x)
    assertNumeric(scale, y)
    checkForOverflow(scale, x.bigDecimal.setScale(scale) add y.bigDecimal)
  }

  def subtract(scale: Int, x: BigDecimal, y: BigDecimal): Either[String, BigDecimal] = {
    assertNumeric(scale, x)
    assertNumeric(scale, y)
    checkForOverflow(scale, x.bigDecimal subtract y.bigDecimal)
  }

  def multiply(scale: Int, x: BigDecimal, y: BigDecimal): Either[String, BigDecimal] = {
    assertNumeric(scale, x)
    assertNumeric(scale, y)
    checkForOverflow(scale, (x.bigDecimal multiply y.bigDecimal).setScale(scale, ROUND_HALF_EVEN))
  }

  def divide(scale: Int, x: BigDecimal, y: BigDecimal): Either[String, BigDecimal] = {
    assertNumeric(scale, x)
    assertNumeric(scale, y)
    checkForOverflow(scale, x.bigDecimal.divide(y.bigDecimal, scale, ROUND_HALF_EVEN))
  }

  def toLong(scale: Int, x: BigDecimal): Either[String, Long] = {
    assertNumeric(scale, x)
    Try(x.bigDecimal.setScale(0, ROUND_DOWN).longValueExact()).toEither.left.map(
      _ => s"(Numeric $scale) $x does not fit into an Int64",
    )
  }

  final def round(scale: Int, newScale: Long, x: BigDecimal): Either[String, BigDecimal] = {
    assertNumeric(scale, x)
    Either.cond(
      newScale <= scale && scale - maxPrecision < newScale,
      x.bigDecimal.setScale(newScale.toInt, ROUND_HALF_EVEN).setScale(scale),
      s"Bad scale $newScale, must be between ${scale - maxPrecision - 1} and $scale"
    )
  }

  final def compare(scale: Int, x: BigDecimal, y: BigDecimal): Int = {
    assertNumeric(scale, x)
    assertNumeric(scale, y)
    x compareTo y
  }

  final def fromBigDecimal(scale: Int, x: BigDecimal): Either[String, BigDecimal] =
    checkScale(scale, x).flatMap(checkForOverflow(scale, _))

  def assertFromBigDecimal(scale: Int, x: BigDecimal): BigDecimal =
    assertRight(fromBigDecimal(scale, x))

  private val ExpectedUnscaledFormat = """[-+]?(\d{1,38})(\.\d{0,38})?""".r

  def unscaledFromString(s: String): Either[String, BigDecimal] =
    Some(s)
      .filter(ExpectedUnscaledFormat.pattern.matcher(_).matches())
      .map(BigDecimal(_))
      .filter(_.precision <= maxPrecision)
      .toRight(
        s"""Could not read Numeric string "$s""""
      )

  def assertUnscaledFromString(s: String): BigDecimal =
    assertRight(unscaledFromString(s))

  def toString(x: BigDecimal): String = {
    val s = x.bigDecimal.stripTrailingZeros.toPlainString
    if (s.contains(".")) s else s + ".0"
  }

  private val ExpectedScaledFormat = """-?([1-9]\d*|0).(\d*)?""".r

  def scaledFromString(s: String): Either[String, BigDecimal] = {
    def errMsg =
      s"""Could not read Numeric string "$s""""
    s match {
      case ExpectedScaledFormat(intPart, decPart) =>
        val scale = decPart.length
        val precision = if (intPart == "0") scale else intPart.size + scale
        if (precision <= maxPrecision) Right(BigDecimal(s).setScale(scale)) else Left(errMsg)
      case _ =>
        Left(errMsg)
    }
  }

  def assertScaledFromString(s: String): BigDecimal =
    assertRight(unscaledFromString(s))

  def toScaledString(x: BigDecimal): String = {
    val s = x.bigDecimal.toPlainString
    if (s.contains(".")) s else s + "."
  }

}
