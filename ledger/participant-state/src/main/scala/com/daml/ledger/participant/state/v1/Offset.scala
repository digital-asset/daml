// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.io.InputStream

import com.daml.lf.data.{Bytes, Ref}

/** Offsets into streams with hierarchical addressing.
  *
  * We use these [[Offset]]'s to address changes to the participant state.
  * Offsets are opaque values that must be must be strictly
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

  def toByteArray: Array[Byte] = bytes.toByteArray

  def toInputStream: InputStream = bytes.toInputStream

  def toHexString: Ref.HexString = bytes.toHexString

  /** Returns the next smaller offset (lexicographically).
    *
    * NOTE: The usage of this method is correct under the assumption that the string length of any offset
    * in the given series is constant (i.e. we imply a finite number of offsets in any given range)
    *
    * @return A new offset with the same underlying byte array size which is one unit smaller
    *          than the current offset.
    */
  def predecessor: Offset = {
    def lexicographicalPredecessor(bytes: Bytes): Bytes = {
      val unsigned = bytes.toByteArray.map(_ & 255)
      val raw = unsigned
        .foldRight((Array.newBuilder[Int], true)) { case (b, (p, carry)) =>
          if (b == 0 && carry) (p.addOne(255), true)
          else (p.addOne(b - { if (carry) 1 else 0 }), false)
        }
        ._1
        .result()
        .map(_.toByte)
      Bytes.fromByteArray(raw.reverse)
    }

    if (bytes.isEmpty || BigInt(toByteArray) == BigInt(0)) {
      val errMsg = this match {
        case Offset.beforeBegin => "Predecessor of beforeBegin not supported"
        case _ => s"Predecessor of $toHexString not supported"
      }
      throw new UnsupportedOperationException(errMsg)
    } else Offset(lexicographicalPredecessor(bytes))
  }
}

object Offset {

  val beforeBegin: Offset = Offset.fromByteArray(Array.empty[Byte])

  def fromByteArray(bytes: Array[Byte]) = new Offset(Bytes.fromByteArray(bytes))

  def fromInputStream(is: InputStream) = new Offset(Bytes.fromInputStream(is))

  def fromHexString(s: Ref.HexString) = new Offset(Bytes.fromHexString(s))
}
