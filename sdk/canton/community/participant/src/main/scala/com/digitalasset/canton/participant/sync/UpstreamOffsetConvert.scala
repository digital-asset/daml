// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.syntax.either.*
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.digitalasset.canton.participant.{GlobalOffset, LedgerSyncOffset}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.{Bytes as LfBytes, Ref}
import com.google.protobuf.ByteString

import java.nio.{ByteBuffer, ByteOrder}

/**  Conversion utility to convert back and forth between GlobalOffsets and the offsets used by the
  *  ParticipantState API ReadService still based on a byte string. Canton emits single-Long GlobalOffsets.
  */
object UpstreamOffsetConvert {
  import com.digitalasset.canton.participant.pretty.Implicits.*

  private val versionUpstreamOffsetsAsLong: Byte = 0
  private val longBasedByteLength: Int = 9 // One byte for the version plus 8 bytes for Long

  def fromGlobalOffset(offset: GlobalOffset): LedgerSyncOffset =
    fromGlobalOffset(offset.toLong)

  def fromGlobalOffset(i: Long) = LedgerSyncOffset(
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

  def toGlobalOffset(offset: LedgerSyncOffset): Either[String, GlobalOffset] = {
    val bytes = offset.bytes.toByteArray
    if (bytes.lengthCompare(longBasedByteLength) != 0) {
      if (offset == LedgerSyncOffset.beforeBegin) {
        Left(s"Invalid canton offset: before ledger begin is not allowed")
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

  def toParticipantOffset(offset: GlobalOffset): ParticipantOffset =
    ParticipantOffset(ParticipantOffset.Value.Absolute(fromGlobalOffset(offset).toHexString))

  def toParticipantOffset(offset: String): ParticipantOffset = ParticipantOffset(
    ParticipantOffset.Value.Absolute(offset)
  )

  def toLedgerSyncOffset(offset: ParticipantOffset): Either[String, LedgerSyncOffset] =
    for {
      absoluteOffset <- Either.cond(
        offset.value.isAbsolute,
        offset.getAbsolute,
        show"offset must be an absolute offset, but received ${offset}",
      )
      ledgerSyncOffset <- toLedgerSyncOffset(absoluteOffset)
    } yield ledgerSyncOffset

  def tryToLedgerSyncOffset(offset: ParticipantOffset): LedgerSyncOffset =
    toLedgerSyncOffset(offset).valueOr(err => throw new IllegalArgumentException(err))

  def tryToLedgerSyncOffset(offset: String): LedgerSyncOffset =
    toLedgerSyncOffset(offset).valueOr(err => throw new IllegalArgumentException(err))

  def toLedgerSyncOffset(offset: String): Either[String, LedgerSyncOffset] =
    Ref.HexString.fromString(offset).map(LedgerSyncOffset.fromHexString)

  def ledgerOffsetToGlobalOffset(ledgerOffset: ParticipantOffset): Either[String, GlobalOffset] =
    for {
      ledgerSyncOffset <- toLedgerSyncOffset(ledgerOffset)
      globalOffset <- toGlobalOffset(ledgerSyncOffset)
    } yield globalOffset
}
