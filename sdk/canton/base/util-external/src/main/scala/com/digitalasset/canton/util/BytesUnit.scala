// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import scala.math.Numeric.LongIsIntegral

final case class BytesUnit(bytes: Long) {

  def *(that: Long): BytesUnit = BytesUnit(this.bytes * that)

  override def toString: String = {
    val factorL = BytesUnit.factor
    val factorD = BytesUnit.factor.toDouble
    val (convertedValue: Double, unit) = bytes match {
      case v if v < factorL => (v.toDouble, "B")
      case v if v >= factorL && v < factorL * factorL => (v / factorD, "KB")
      case v if v >= factorL * factorL && v < factorL * factorL * factorL =>
        (v / (factorD * factorD), "MB")
      case v =>
        (v / (factorD * factorD * factorD), "GB")
    }
    f"$convertedValue%.2f $unit"
  }
}

object BytesUnit {
  private[BytesUnit] val factor = 1024L

  val zero: BytesUnit = BytesUnit(0L)

  def KB(value: Long): BytesUnit = BytesUnit(value) * factor
  def MB(value: Long): BytesUnit = KB(value) * factor

  implicit val bytesUnitIsNumeric: Numeric[BytesUnit] = new Numeric[BytesUnit] {
    override def plus(x: BytesUnit, y: BytesUnit): BytesUnit = BytesUnit(x.bytes + y.bytes)

    override def minus(x: BytesUnit, y: BytesUnit): BytesUnit = BytesUnit(x.bytes - y.bytes)

    override def times(x: BytesUnit, y: BytesUnit): BytesUnit = BytesUnit(x.bytes * y.bytes)

    override def negate(x: BytesUnit): BytesUnit = BytesUnit(-x.bytes)

    override def fromInt(x: Int): BytesUnit = BytesUnit(x.toLong)

    override def parseString(str: String): Option[BytesUnit] =
      LongIsIntegral.parseString(str).map(BytesUnit(_))

    override def toInt(x: BytesUnit): Int = x.bytes.toInt

    override def toLong(x: BytesUnit): Long = x.bytes

    override def toFloat(x: BytesUnit): Float = x.bytes.toFloat

    override def toDouble(x: BytesUnit): Double = x.bytes.toDouble

    override def compare(x: BytesUnit, y: BytesUnit): Int = java.lang.Long.compare(x.bytes, y.bytes)
  }

}
