// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.Offset.beforeBegin
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.google.protobuf.ByteString

import java.io.InputStream
import java.nio.{ByteBuffer, ByteOrder}

/** Offsets into streams with hierarchical addressing.
  *
  * We use these [[Offset]]'s to address changes to the participant state.
  * Offsets are opaque values that must be strictly
  * increasing according to lexicographical ordering.
  *
  * Ledger implementations are advised to future proof their design
  * of offsets by reserving the first (few) bytes for a version
  * indicator, followed by the specific offset scheme for that version.
  * This way it is possible in the future to switch to a different versioning
  * scheme, while making sure that previously created offsets are always
  * less than newer offsets.
  */
final case class Offset(bytes: Bytes) extends Ordered[Offset] {
  override def compare(that: Offset): Int =
    Bytes.ordering.compare(this.bytes, that.bytes)

  def toByteString: ByteString = bytes.toByteString

  def toByteArray: Array[Byte] = bytes.toByteArray

  def toHexString: Ref.HexString = bytes.toHexString

  def toLong: Long =
    if (this == beforeBegin) 0L
    else ByteBuffer.wrap(bytes.toByteArray).getLong(1)

  // TODO(#21220) remove after the unification of Offsets
  def toAbsoluteOffsetO: Option[AbsoluteOffset] =
    if (this == beforeBegin) None
    else Some(AbsoluteOffset.tryFromLong(this.toLong))

  // TODO(#21220) remove after the unification of Offsets
  def toAbsoluteOffset: AbsoluteOffset = AbsoluteOffset.tryFromLong(this.toLong)
}

/** Offsets into streams with hierarchical addressing.
  *
  * We use these offsets to address changes to the participant state.
  * Offsets are opaque values that must be strictly increasing.
  */
// TODO(#21220) rename to Offset
class AbsoluteOffset private (val positive: Long) extends AnyVal with Ordered[AbsoluteOffset] {
  def unwrap: Long = positive

  def compare(that: AbsoluteOffset): Int = this.positive.compare(that.positive)

  override def toString: String = s"AbsoluteOffset($positive)"

  // TODO(#22143) remove after Offsets are stored as integers in db
  def toHexString: String = Offset.fromLong(positive).toHexString

  // TODO(#21220) remove after the unification of Offsets
  def toOffset: Offset = Offset.fromLong(this.unwrap)
}

object AbsoluteOffset {
  lazy val firstOffset: AbsoluteOffset = AbsoluteOffset.tryFromLong(1L)
  lazy val MaxValue: AbsoluteOffset = AbsoluteOffset.tryFromLong(Long.MaxValue)
  val beforeBegin: Option[AbsoluteOffset] = None

  def tryFromLong(num: Long): AbsoluteOffset =
    fromLong(num).valueOr(err => throw new IllegalArgumentException(err))

  def fromLong(num: Long): Either[String, AbsoluteOffset] =
    Either.cond(
      num > 0L,
      new AbsoluteOffset(num),
      s"Expecting positive value for offset, found $num.",
    )

  implicit val `AbsoluteOffset to LoggingValue`: ToLoggingValue[AbsoluteOffset] = value =>
    LoggingValue.OfLong(value.unwrap)

  implicit class AbsoluteOffsetOptionToHexString(val offsetO: Option[AbsoluteOffset])
      extends AnyVal {
    def toHexString: String = offsetO match {
      case Some(offset) => offset.toHexString
      case None => ""
    }
  }
}

object Offset {
  val beforeBegin: Offset = new Offset(Bytes.Empty)
  private val longBasedByteLength: Int = 9 // One byte for the version plus 8 bytes for Long
  private val versionUpstreamOffsetsAsLong: Byte = 0
  val firstOffset: Offset = Offset.fromLong(1)

  def fromByteString(bytes: ByteString) = new Offset(Bytes.fromByteString(bytes))

  def fromByteArray(bytes: Array[Byte]) = new Offset(Bytes.fromByteArray(bytes))

  def fromInputStream(is: InputStream) = new Offset(Bytes.fromInputStream(is))

  def fromHexString(s: Ref.HexString) = new Offset(Bytes.fromHexString(s))

  def fromLong(l: Long): Offset =
    if (l == 0L) beforeBegin
    else
      Offset(
        com.digitalasset.daml.lf.data.Bytes.fromByteString(
          ByteString.copyFrom(
            ByteBuffer
              .allocate(longBasedByteLength)
              .order(ByteOrder.BIG_ENDIAN)
              .put(0, versionUpstreamOffsetsAsLong)
              .putLong(1, l)
          )
        )
      )

  // TODO(#21220) remove after the unification of Offsets
  def fromAbsoluteOffsetO(valueO: Option[AbsoluteOffset]): Offset =
    valueO match {
      case None => beforeBegin
      case Some(value) => fromLong(value.unwrap)
    }

  // TODO(#21220) remove after the unification of Offsets
  def fromAbsoluteOffset(value: AbsoluteOffset): Offset = fromLong(value.unwrap)

  implicit val `Offset to LoggingValue`: ToLoggingValue[Offset] = value =>
    LoggingValue.OfLong(value.toLong)
}
