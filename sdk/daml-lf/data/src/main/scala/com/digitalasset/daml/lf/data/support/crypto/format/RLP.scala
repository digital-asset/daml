// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package support.crypto.format

import java.nio.{ByteBuffer, ByteOrder}

/** Recursive Length Prefix (RLP) encoding.
  *
  * See: https://ethereum-classic-guide.readthedocs.io/en/latest/docs/appendices/recursive_length_prefix.html
  */
object RLP {

  def encode(tree: RLPTree): Bytes = tree match {
    case RLPBlock(data) =>
      val header = if (data.length == 1 && data.unsignedByteAt(0) < 128) {
        Bytes.Empty
      } else if (data.length < 56) {
        Bytes.fromByte((data.length + 128).toByte)
      } else {
        encodeLen(data, 183)
      }
      header ++ data

    case RLPList(data @ _*) =>
      val encodedData = data.foldLeft(Bytes.Empty) { case (body, node) =>
        body ++ encode(node)
      }
      val header = if (encodedData.length < 56) {
        Bytes.fromByte((encodedData.length + 192).toByte)
      } else {
        encodeLen(encodedData, 247)
      }
      header ++ encodedData
  }

  def decode(data: Bytes): RLPTree = {
    if (data.unsignedByteAt(0) < 128) {
      RLPBlock(data)
    } else if (data.unsignedByteAt(0) < 184) {
      RLPBlock(data.tail)
    } else if (data.unsignedByteAt(0) < 192) {
      RLPBlock(data.drop(data.unsignedByteAt(0) - 183))
    } else {
      var bytes = if (data.unsignedByteAt(0) < 248) {
        data.tail
      } else {
        data.drop(data.unsignedByteAt(0) - 247)
      }
      var nodes = Seq.empty[RLPTree]

      while (bytes.nonEmpty) {
        val len = if (bytes.unsignedByteAt(0) < 128) {
          1
        } else if (bytes.unsignedByteAt(0) < 184) {
          1 + bytes.unsignedByteAt(0) - 128
        } else if (bytes.unsignedByteAt(0) < 192) {
          decodeLen(bytes, 183)
        } else if (bytes.unsignedByteAt(0) < 248) {
          1 + bytes.unsignedByteAt(0) - 192
        } else {
          decodeLen(bytes, 247)
        }

        nodes = nodes :+ decode(bytes.slice(0, len))
        bytes = bytes.drop(len)
      }

      RLPList(nodes: _*)
    }
  }

  private def encodeLen(data: Bytes, extra: Int): Bytes = {
    val len = data.length
    // Ethereum encodes integers in big endian format with no leading zeros.
    // So, integer 0 would be represented as Bytes.Empty
    if (len == 0) {
      Bytes.fromByte(extra.toByte)
    } else {
      val numLenBits = if (len == 1) 1 else Math.ceil(Math.log10(len.toDouble) / Math.log10(2))
      val numLenBytes = Math.ceil(numLenBits / 8).toInt
      val lenBytes = Bytes.fromByteArray(
        ByteBuffer.allocate(numLenBytes).order(ByteOrder.BIG_ENDIAN).putInt(len).array()
      )

      Bytes.fromByte((lenBytes.length + extra).toByte) ++ lenBytes
    }
  }

  private def decodeLen(lenBytes: Bytes, extra: Int): Int = {
    val numLenBytes = lenBytes.unsignedByteAt(0) - extra

    1 + numLenBytes + ByteBuffer
      .wrap(lenBytes.slice(2, 2 + numLenBytes).toByteArray)
      .order(ByteOrder.BIG_ENDIAN)
      .getInt()
  }

  sealed abstract class RLPTree extends Product with Serializable
  final case class RLPBlock(data: Bytes) extends RLPTree
  final case class RLPList(data: RLPTree*) extends RLPTree
}
