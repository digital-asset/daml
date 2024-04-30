// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.HexString
import com.google.protobuf.ByteString

import java.nio.{ByteBuffer, ByteOrder, ShortBuffer}
import scala.concurrent.blocking
import scala.jdk.CollectionConverters.*

/** A running digest of a set of bytes, where elements can be added and removed.
  *
  * Note that it's the caller's responsibility to ensure that the collection defined by the sequence of
  * additions/removals is really a set. In particular:
  * 1. the digest accepts a call to [[remove]] before the corresponding call to [[add]]
  * 2. the digest will change if the same element is added twice. Note, however, that the digest rolls over if you
  *    add an element 2^16 times; i.e., taking a digest d, then adding the same element 2^16 times results in d again.
  */
class LtHash16 private (private val buffer: Array[Byte]) {
  import LtHash16.*

  require(
    buffer.length == BYTE_LENGTH,
    s"Can't initialize LtHash16 from the given ${buffer.length} bytes",
  )

  private def shortBuffer(bytes: Array[Byte]): ShortBuffer =
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer

  private val lock = new Object

  private def blockSync[T]: T => T = x => blocking(lock.synchronized(x))

  private def performOp(bytes: Array[Byte], f: (Short, Short) => Int): Unit = blockSync {
    val hash = Blake2xb.digest(bytes, BYTE_LENGTH)
    val hBuf = shortBuffer(hash)
    // Internal buffer
    val iBuf = shortBuffer(buffer)
    for (i <- 0 until VECTOR_LENGTH) {
      val newVal = f(iBuf.get(i), hBuf.get(i))
      // Note that the potential loss of the highest bit due to conversion to short is intentional here, as this
      // gives us the desired semantics of addition modulo 2^16.
      iBuf.put(i, newVal.toShort).discard[ShortBuffer]
    }
  }

  def add(bytes: Array[Byte]): Unit = performOp(bytes, _ + _)

  def remove(bytes: Array[Byte]): Unit = performOp(bytes, _ - _)

  def get(): Array[Byte] = blockSync {
    buffer.clone()
  }

  def getByteString(): ByteString = blockSync {
    ByteString.copyFrom(buffer)
  }

  def isEmpty: Boolean = blockSync {
    buffer.forall(_ == 0)
  }

  def hexString(): String = HexString.toHexString(buffer)
}

object LtHash16 {
  private val VECTOR_LENGTH = 1024
  private val SIZEOF_SHORT = 2
  private val BYTE_LENGTH = VECTOR_LENGTH * SIZEOF_SHORT

  def apply(): LtHash16 = new LtHash16(
    new Array[Byte](BYTE_LENGTH)
  ) // Initialized to all 0s by default

  /** Try to create a hash from the bytes produced by [[com.digitalasset.canton.crypto.LtHash16.get]]
    * @param bytes Serialized byte array of an LtHash16
    * @throws java.lang.IllegalArgumentException if the given bytes cannot be interpreted as a legal LtHash16
    */
  def tryCreate(bytes: Array[Byte]) = new LtHash16(bytes)

  /** Try to create a hash from the bytes produced by [[com.digitalasset.canton.crypto.LtHash16.get]]
    * @param bytes Serialized ByteString of an LtHash16
    * @throws java.lang.IllegalArgumentException if the given bytes cannot be interpreted as a legal LtHash16
    */
  def tryCreate(bytes: ByteString) = new LtHash16(bytes.toByteArray)

  def isNonEmptyCommitment(bytes: ByteString): Boolean = {
    bytes.size() == BYTE_LENGTH && bytes.asScala.exists(_ != 0)
  }
}
