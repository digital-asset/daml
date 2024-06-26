// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.Offset.beforeBegin
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.google.protobuf.ByteString

import java.io.InputStream
import java.nio.{ByteBuffer, ByteOrder}

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

  implicit val `Offset to LoggingValue`: ToLoggingValue[Offset] = value =>
    LoggingValue.OfString(value.toHexString)
}
