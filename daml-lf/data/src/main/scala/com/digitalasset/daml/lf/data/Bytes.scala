// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import java.io.InputStream
import java.nio.ByteBuffer

import com.google.protobuf.ByteString

final class Bytes private (protected val value: ByteString) extends AnyVal {

  def toByteArray: Array[Byte] = value.toByteArray

  def toByteString: ByteString = value

  def toByteBuffer: ByteBuffer = value.asReadOnlyByteBuffer()

  def toInputStream: InputStream = value.newInput()

  def length: Int = value.size()

  def isEmpty: Boolean = value.isEmpty

  def nonEmpty: Boolean = !value.isEmpty

  def toHexString: Ref.HexString = Ref.HexString.encode(this)

  def startsWith(prefix: Bytes): Boolean = value.startsWith(prefix.value)

  def slice(begin: Int, end: Int): Bytes = new Bytes(value.substring(begin, end))

  override def toString: String = s"Bytes($toHexString)"

  def ++(that: Bytes) = new Bytes(this.value.concat(that.value))
}

object Bytes {

  val Empty = new Bytes(ByteString.EMPTY)

  implicit val ordering: Ordering[Bytes] = {
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
    Ref.HexString.decode(s)

  def fromString(s: String): Either[String, Bytes] =
    Ref.HexString.fromString(s).map(fromHexString)

  def assertFromString(s: String): Bytes =
    assertRight(fromString(s))

}
