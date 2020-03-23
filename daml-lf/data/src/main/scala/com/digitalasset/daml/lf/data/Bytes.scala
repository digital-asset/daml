// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.io.InputStream
import java.nio.ByteBuffer

import com.google.protobuf.ByteString

final class Bytes private (private val value: ByteString) extends AnyVal {

  def toByteArray: Array[Byte] = value.toByteArray

  def toByteString: ByteString = value

  def toByteBuffer: ByteBuffer = value.asReadOnlyByteBuffer()

  def toInputStream: InputStream = value.newInput()

  def length: Int = value.size()

  def toHexString: Ref.HexString = Ref.HexString.encode(this)

  override def toString: String = s"Bytes($toHexString)"
}

object Bytes {

  implicit val `Bytes Ordering`: Ordering[Bytes] = {
    val comparator = ByteString.unsignedLexicographicalComparator()
    (x, y) =>
      comparator.compare(x.value, y.value)
  }

  def fromByteString(value: ByteString): Bytes =
    new Bytes(value)

  def fromByteArray(a: Array[Byte]): Bytes =
    new Bytes(ByteString.copyFrom(a))

  def fromByteBuffer(a: ByteBuffer): Bytes =
    new Bytes(ByteString.copyFrom(a))

  def fromInputStream(a: InputStream): Bytes =
    new Bytes(ByteString.readFrom(a))

  def fromHexString(s: Ref.HexString): Bytes =
    Ref.HexString
      .decode(s)

  def fromString(s: String): Either[String, Bytes] =
    Ref.HexString.fromString(s).map(fromHexString)

  def assertFromString(s: String): Bytes =
    assertRight(fromString(s))

}
