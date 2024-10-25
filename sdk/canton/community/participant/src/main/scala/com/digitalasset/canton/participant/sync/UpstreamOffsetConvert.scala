// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.syntax.either.*
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.daml.lf.data.{Bytes as LfBytes, Ref}
import com.google.protobuf.ByteString

import java.nio.{ByteBuffer, ByteOrder}

/**  Conversion utility to convert back and forth between GlobalOffsets and the offsets used by the
  *  ParticipantState API ReadService still based on a byte string. Canton emits single-Long GlobalOffsets.
  */
object UpstreamOffsetConvert {

  private val versionUpstreamOffsetsAsLong: Byte = 0
  private val longBasedByteLength: Int = 9 // One byte for the version plus 8 bytes for Long

  def fromGlobalOffset(offset: GlobalOffset): Offset =
    fromGlobalOffset(offset.toLong)

  def fromGlobalOffset(i: Long) = Offset(
    LfBytes.fromByteString(
      ByteString.copyFrom(
        ByteBuffer
          .allocate(longBasedByteLength)
          .order(ByteOrder.BIG_ENDIAN)
          .put(0, versionUpstreamOffsetsAsLong)
          .putLong(1, i)
      )
    )
  )

  def toGlobalOffset(offset: Offset): Either[String, GlobalOffset] = {
    val bytes = offset.bytes.toByteArray
    if (bytes.lengthCompare(longBasedByteLength) != 0) {
      if (offset == Offset.beforeBegin) {
        Left("Invalid canton offset: before ledger begin is not allowed")
      } else {
        Left(s"Invalid canton offset length: expected $longBasedByteLength, actual ${bytes.length}")
      }
    } else if (!bytes.headOption.contains(versionUpstreamOffsetsAsLong)) {
      Left(
        s"Unknown canton offset version: Expected $versionUpstreamOffsetsAsLong, actual ${bytes.headOption}"
      )
    } else {
      val rawOffset = ByteBuffer.wrap(bytes).getLong(1)

      GlobalOffset.fromLong(rawOffset)
    }
  }

  def toStringOffset(offset: GlobalOffset): String =
    fromGlobalOffset(offset).toHexString

  def tryToLedgerSyncOffset(offset: String): Offset =
    toLedgerSyncOffset(offset).valueOr(err => throw new IllegalArgumentException(err))

  def tryToLedgerSyncOffset(offset: Long): Offset =
    Offset.fromLong(offset)

  def toLedgerSyncOffset(offset: String): Either[String, Offset] =
    Ref.HexString.fromString(offset).map(Offset.fromHexString)

  def toLedgerSyncOffset(offset: Long): Either[String, Offset] =
    try {
      Right(tryToLedgerSyncOffset(offset))
    } catch {
      case e: Throwable => Left(e.getMessage)
    }
}
