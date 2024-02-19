// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.traffic.v30.MemberTrafficStatus.TopUpEvent as TopUpEventP
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter
import slick.jdbc.GetResult

object TopUpEvent {
  implicit val ordering: Ordering[TopUpEvent] = {
    // Order first by timestamp then by serial number to differentiate if necessary
    (x: TopUpEvent, y: TopUpEvent) =>
      {
        x.validFromInclusive compare y.validFromInclusive match {
          case 0 => x.serial compare y.serial
          case c => c
        }
      }
  }

  import com.digitalasset.canton.store.db.RequiredTypesCodec.*

  implicit val topUpEventGetResult: GetResult[TopUpEvent] =
    GetResult.createGetTuple3[CantonTimestamp, PositiveLong, PositiveInt].andThen {
      case (ts, limit, sc) =>
        TopUpEvent(limit, ts, sc)
    }

  implicit class EnhancedOption(val limitOpt: Option[TopUpEvent]) extends AnyVal {
    def asNonNegative: NonNegativeLong =
      limitOpt.map(_.limit.toNonNegative).getOrElse(NonNegativeLong.zero)
  }

  def fromProtoV30(
      topUp: TopUpEventP
  ): Either[ProtoDeserializationError, TopUpEvent] = {
    for {
      limit <- ProtoConverter.parsePositiveLong(topUp.extraTrafficLimit)
      serial <- ProtoConverter.parsePositiveInt(topUp.serial)
      validFrom <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "effective_at",
        topUp.effectiveAt,
      )
    } yield TopUpEvent(
      limit,
      validFrom,
      serial,
    )
  }
}

final case class TopUpEvent(
    limit: PositiveLong,
    validFromInclusive: CantonTimestamp,
    serial: PositiveInt,
) {
  def toProtoV30: TopUpEventP = {
    TopUpEventP(
      Some(validFromInclusive.toProtoTimestamp),
      serial = serial.value,
      extraTrafficLimit = limit.value,
    )
  }
}
