// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.io.InputStream

import com.google.protobuf.ByteString

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
  *
  */
final class Offset(private val value: ByteString) extends AnyVal with Ordered[Offset] {

  override def compare(that: Offset): Int =
    Offset.comparator.compare(value, that.value)

  def toByteArray: Array[Byte] = value.toByteArray

  def toInputStream: InputStream = value.newInput()
}

object Offset {
  private val comparator = ByteString.unsignedLexicographicalComparator()

  val begin: Offset = new Offset(ByteString.copyFrom(Array(0: Byte)))

  def fromBytes(bytes: Array[Byte]) = new Offset(ByteString.copyFrom(bytes))

  def fromInputStream(is: InputStream) = new Offset(ByteString.readFrom(is))
}
