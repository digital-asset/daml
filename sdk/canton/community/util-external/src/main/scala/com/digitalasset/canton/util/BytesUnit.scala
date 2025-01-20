// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong

import BytesUnit.*

sealed trait BytesUnit {
  def toBytes: Bytes

  def *(that: NonNegativeLong): Bytes = Bytes(this.toBytes.value * that)
  def <=(that: BytesUnit): Boolean = this.toBytes.value <= that.toBytes.value

  override def toString: String = {
    val (convertedValue: Double, unit) = toBytes.value.unwrap match {
      case v if v < 1024 => (v.toDouble, "B")
      case v if v >= 1024 && v < 1024 * 1024 => (v / 1024.0, "KB")
      case v if v >= 1024 * 1024 && v < 1024 * 1024 * 1024 => (v / (1024.0 * 1024.0), "MB")
      case v =>
        (v / (1024.0 * 1024.0 * 1024.0), "GB")
    }
    f"$convertedValue%.2f $unit"
  }

}
object BytesUnit {
  val factor = NonNegativeLong.tryCreate(1024)

  final case class Bytes(value: NonNegativeLong) extends BytesUnit {
    override def toBytes: Bytes = Bytes(value)
  }
  object Bytes {
    def apply(value: Long): Bytes = Bytes(NonNegativeLong.tryCreate(value))
  }

  final case class Kilobytes(value: NonNegativeLong) extends BytesUnit {
    override def toBytes: Bytes = Bytes((value * factor))
  }

  final case class Megabytes(value: NonNegativeLong) extends BytesUnit {
    override def toBytes: Bytes = Bytes((value * factor * factor))
  }

  final case class Gigabytes(value: NonNegativeLong) extends BytesUnit {
    override def toBytes: Bytes = Bytes((value * factor * factor * factor))
  }
}
