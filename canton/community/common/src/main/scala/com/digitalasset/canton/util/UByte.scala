// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.either.*
import com.google.protobuf.ByteString
import io.circe.{Decoder, Encoder}

// This class provides a helper for unsigned bytes as java/scala bytes are signed.
final case class UByte(signed: Byte) extends NoCopy {
  def unsigned: Int = signed & 0xff
}

object UByte {
  implicit val encoder: Encoder[UByte] = Encoder.encodeInt.contramap(_.unsigned)
  implicit val decoder: Decoder[UByte] = Decoder.decodeInt.emap(int => UByte.fromUnsignedInt(int))

  def fromSignedInt(int: Int): Either[String, UByte] =
    Either.cond(
      int >= -128 && int <= 127,
      new UByte(int.toByte),
      s"Signed int $int outside of byte range",
    )

  def tryFromSignedInt(int: Int): UByte =
    fromSignedInt(int).valueOr(err =>
      throw new IllegalArgumentException(s"Invalid signed integer for UByte: $err")
    )

  def fromUnsignedInt(int: Int): Either[String, UByte] =
    Either.cond(
      int >= 0 && int <= 255,
      new UByte(int.toByte),
      s"Unsigned int $int outside of byte range",
    )

  def tryFromUnsignedInt(int: Int): UByte =
    fromUnsignedInt(int).valueOr(err =>
      throw new IllegalArgumentException(s"Invalid unsigned integer for UByte: $err")
    )

  def fromArrayToByteString(array: Array[UByte]): ByteString =
    ByteString.copyFrom(array.map(_.signed))

  def fromByteStringToVector(bytes: ByteString): Vector[UByte] =
    bytes.toByteArray.toVector.map(UByte(_))
}
