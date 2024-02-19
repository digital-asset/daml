// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.google.protobuf.ByteString

/** Conversion functions to and from hex strings. */
object HexString {

  def toHexString(bytes: ByteString): String = toHexString(bytes.toByteArray)

  /** Convert a ByteString to hex-string.
    * The output size will be equal to the length configured if it's even, or
    * to the length + 1 if it's odd.
    */
  def toHexString(bytes: ByteString, length: Int): String = {
    // Every byte is 2 Hex characters, this is why we devise by 2
    val maxlength = bytes.size() min Math.round(length / 2.toDouble).toInt
    toHexString(
      bytes.substring(0, maxlength).toByteArray
    )
  }

  def toHexString(bytes: Array[Byte]): String = bytes.map(b => f"$b%02x").mkString("")

  /** Parse a hex-string `s` to a byte array. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def parse(s: String): Option[Array[Byte]] = {
    val optBytes: Iterator[Option[Byte]] = s
      .grouped(2)
      .map(s2 =>
        for {
          _ <- if (s2.lengthCompare(2) != 0) None else Some(())
          b1 = Character.digit(s2.charAt(0), 16)
          b2 = Character.digit(s2.charAt(1), 16)
          _ <- if (b1 == -1 || b2 == -1) None else Some(())
          b = ((b1 << 4) + b2).asInstanceOf[Byte]
        } yield b
      )
    optBytes
      .foldRight(Option.apply(Seq[Byte]())) { case (bOrErr, bytesOrErr) =>
        for {
          b <- bOrErr
          bytes <- bytesOrErr
        } yield bytes.+:(b)
      }
      .map(_.toArray)
  }

  def parseToByteString(s: String): Option[ByteString] = {
    parse(s).map(ByteString.copyFrom)
  }
}
