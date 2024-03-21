// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.data.{Bytes as LfBytes}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.serialization.ProtoConverter.{DurationConverter, ParsingResult}

final case class SerializableDeduplicationPeriod(deduplicationPeriod: DeduplicationPeriod) {
  def toProtoV0: v0.DeduplicationPeriod = deduplicationPeriod match {
    case duration: DeduplicationPeriod.DeduplicationDuration =>
      v0.DeduplicationPeriod(
        v0.DeduplicationPeriod.Period.Duration(
          DurationConverter.toProtoPrimitive(duration.duration)
        )
      )
    case offset: DeduplicationPeriod.DeduplicationOffset =>
      v0.DeduplicationPeriod(v0.DeduplicationPeriod.Period.Offset(offset.offset.bytes.toByteString))
  }
}
object SerializableDeduplicationPeriod {
  def fromProtoV0(
      deduplicationPeriodP: v0.DeduplicationPeriod
  ): ParsingResult[DeduplicationPeriod] = {
    val dedupP = v0.DeduplicationPeriod.Period
    deduplicationPeriodP.period match {
      case dedupP.Empty => Left(ProtoDeserializationError.FieldNotSet("DeduplicationPeriod.value"))
      case dedupP.Duration(duration) =>
        DurationConverter
          .fromProtoPrimitive(duration)
          .map(DeduplicationPeriod.DeduplicationDuration)
      case dedupP.Offset(offset) =>
        Right(DeduplicationPeriod.DeduplicationOffset(Offset(LfBytes.fromByteString(offset))))
    }
  }
}
