// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.AbsoluteOffset.{firstOffset, tryFromLong}
import com.digitalasset.canton.data.Offset.beforeBegin
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.pretty.Pretty.prettyOfString
import com.digitalasset.canton.pekkostreams.dispatcher.DispatcherImpl.Incrementable
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

import java.io.InputStream
import java.nio.{ByteBuffer, ByteOrder}
import scala.util.{Failure, Success, Try}

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
class AbsoluteOffset private (val positive: Long)
    extends AnyVal
    with Ordered[AbsoluteOffset]
    with Incrementable[AbsoluteOffset] {
  def unwrap: Long = positive

  def compare(that: AbsoluteOffset): Int = this.positive.compare(that.positive)

  def increment: AbsoluteOffset = tryFromLong(positive + 1L)

  def min(other: AbsoluteOffset): AbsoluteOffset = new AbsoluteOffset(positive.min(other.unwrap))

  def max(other: AbsoluteOffset): AbsoluteOffset = new AbsoluteOffset(positive.max(other.unwrap))

  // TODO(#21220) remove after db operations use inclusive start
  def decrement: Option[AbsoluteOffset] =
    Option.unless(this == firstOffset)(tryFromLong(positive - 1L))

  override def toString: String = s"AbsoluteOffset($positive)"

  def toDecimalString: String = positive.toString

  // TODO(#22143) remove after Offsets are stored as integers in db
  def toHexString: Ref.HexString = Offset.fromLong(positive).toHexString

  // TODO(#21220) remove after the unification of Offsets
  def toOffset: Offset = Offset.fromLong(this.unwrap)
}

object AbsoluteOffset {
  lazy val firstOffset: AbsoluteOffset = AbsoluteOffset.tryFromLong(1L)
  lazy val MaxValue: AbsoluteOffset = AbsoluteOffset.tryFromLong(Long.MaxValue)

  def tryFromLong(num: Long): AbsoluteOffset =
    fromLong(num).valueOr(err => throw new IllegalArgumentException(err))

  def fromLong(num: Long): Either[String, AbsoluteOffset] =
    Either.cond(
      num > 0L,
      new AbsoluteOffset(num),
      s"Expecting positive value for offset, found $num.",
    )

  def fromHexString(s: Ref.HexString): AbsoluteOffset = {
    val bytes = Bytes.fromHexString(s)
    AbsoluteOffset.tryFromLong(ByteBuffer.wrap(bytes.toByteArray).getLong(1))
  }

  def fromHexStringO(s: Ref.HexString): Option[AbsoluteOffset] =
    if (s.isEmpty) None else Some(fromHexString(s))

  def tryFromString(s: String): Try[Option[AbsoluteOffset]] =
    fromString(s) match {
      case Left(msg) => Failure(new IllegalArgumentException(msg))
      case Right(offset) => Success(offset)
    }

  private def fromString(s: String): Either[String, Option[AbsoluteOffset]] =
    Ref.HexString
      .fromString(s)
      .map(AbsoluteOffset.fromHexStringO)

  implicit val `AbsoluteOffset to LoggingValue`: ToLoggingValue[AbsoluteOffset] = value =>
    LoggingValue.OfLong(value.unwrap)

  implicit class AbsoluteOffsetOptionToHexString(val offsetO: Option[AbsoluteOffset])
      extends AnyVal {
    def toHexString: Ref.HexString = offsetO match {
      case Some(offset) => offset.toHexString
      case None => Bytes.Empty.toHexString
    }
  }

  implicit val prettyOffset: Pretty[AbsoluteOffset] = prettyOfString(_.toDecimalString)

  implicit val getResultOffset: GetResult[AbsoluteOffset] =
    GetResult(_.nextLong()).andThen(AbsoluteOffset.tryFromLong)
  implicit val getResultOffsetO: GetResult[Option[AbsoluteOffset]] =
    GetResult(_.nextLongOption().map(AbsoluteOffset.tryFromLong))

  implicit val setParameterOffset: SetParameter[AbsoluteOffset] = (off, pp) => pp >> off.unwrap
  implicit val setParameterOffsetO: SetParameter[Option[AbsoluteOffset]] = (off, pp) =>
    pp >> off.map(_.unwrap)

}

object Offset {
  val beforeBegin: Offset = new Offset(Bytes.Empty)
  private val longBasedByteLength: Int = 9 // One byte for the version plus 8 bytes for Long
  private val versionUpstreamOffsetsAsLong: Byte = 0
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
}
