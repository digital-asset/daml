// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.offset

import java.io.InputStream

import com.daml.lf.data.{Bytes, Ref}
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
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
  */
final case class Offset(bytes: Bytes) extends Ordered[Offset] {
  override def compare(that: Offset): Int =
    Bytes.ordering.compare(this.bytes, that.bytes)

  def toByteString: ByteString = bytes.toByteString

  def toByteArray: Array[Byte] = bytes.toByteArray

  def toInputStream: InputStream = bytes.toInputStream

  def toHexString: Ref.HexString = bytes.toHexString
}

object Offset {
  val beforeBegin: Offset = new Offset(Bytes.Empty)

  def fromByteString(bytes: ByteString) = new Offset(Bytes.fromByteString(bytes))

  def fromByteArray(bytes: Array[Byte]) = new Offset(Bytes.fromByteArray(bytes))

  def fromInputStream(is: InputStream) = new Offset(Bytes.fromInputStream(is))

  def fromHexString(s: Ref.HexString) = new Offset(Bytes.fromHexString(s))

  implicit val `Offset to LoggingValue`: ToLoggingValue[Offset] = value =>
    LoggingValue.OfString(value.toHexString)
}
