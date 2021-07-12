// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import java.io.InputStream
import java.nio.ByteBuffer
import scalaz.Order
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
    (x, y) => comparator.compare(x.value, y.value)
  }

  implicit val order: Order[Bytes] = Order.fromScalaOrdering

  def fromByteString(value: ByteString): Bytes =
    new Bytes(value)

  def fromByteArray(a: Array[Byte]): Bytes =
    fromByteArray(a, 0, a.length)

  def fromByteArray(a: Array[Byte], offset: Int, size: Int) =
    new Bytes(ByteString.copyFrom(a, offset, size))

  def fromByteBuffer(a: ByteBuffer): Bytes =
    new Bytes(ByteString.copyFrom(a))

  def fromInputStream(a: InputStream): Bytes =
    new Bytes(ByteString.readFrom(a))

  // reads at most `len` bytes, does not close `a`
  def fromInputStream(a: InputStream, len: Int): Bytes = {
    if (len < 0) throw new IndexOutOfBoundsException()
    val b = new InputStream {
      private[this] var rest = len
      override def read(): Int =
        if (rest == 0) {
          -1
        } else {
          val c = a.read()
          if (c >= 0) rest -= 1
          c
        }
      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        if (rest == 0 && len > 0) {
          -1
        } else {
          val n = a.read(b, off, len min rest)
          if (n >= 0) rest -= n else rest = 0
          n
        }
      }
    }
    fromInputStream(b)
  }

  def fromHexString(s: Ref.HexString): Bytes =
    Ref.HexString.decode(s)

  def fromString(s: String): Either[String, Bytes] =
    Ref.HexString.fromString(s).map(fromHexString)

  def assertFromString(s: String): Bytes =
    assertRight(fromString(s))

}
