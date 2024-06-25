// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.apply.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.traffic.{
  TrafficConsumed,
  TrafficPurchased,
  TrafficReceipt,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.db.RequiredTypesCodec.nonNegativeLongOptionGetResult
import com.digitalasset.canton.topology.Member
import slick.jdbc.{GetResult, SetParameter}

/** Traffic state of a member at a given timestamp */
final case class TrafficState(
    extraTrafficPurchased: NonNegativeLong,
    extraTrafficConsumed: NonNegativeLong,
    baseTrafficRemainder: NonNegativeLong,
    lastConsumedCost: NonNegativeLong,
    timestamp: CantonTimestamp,
    serial: Option[PositiveInt],
) extends PrettyPrinting {
  def extraTrafficRemainder: Long = extraTrafficPurchased.value - extraTrafficConsumed.value
  // Need big decimal here because it could overflow a long especially if extraTrafficPurchased == Long.MAX
  lazy val availableTraffic: BigDecimal =
    BigDecimal(extraTrafficRemainder) + BigDecimal(baseTrafficRemainder.value)

  def toProtoV30: v30.TrafficState = v30.TrafficState(
    extraTrafficPurchased = extraTrafficPurchased.value,
    extraTrafficConsumed = extraTrafficConsumed.value,
    baseTrafficRemainder = baseTrafficRemainder.value,
    lastConsumedCost = lastConsumedCost.value,
    timestamp = timestamp.toProtoPrimitive,
    serial = serial.map(_.value),
  )

  def toTrafficConsumed(member: Member): TrafficConsumed =
    TrafficConsumed(
      member = member,
      sequencingTimestamp = timestamp,
      extraTrafficConsumed = extraTrafficConsumed,
      baseTrafficRemainder = baseTrafficRemainder,
      lastConsumedCost = lastConsumedCost,
    )

  def toTrafficReceipt: TrafficReceipt = TrafficReceipt(
    consumedCost = lastConsumedCost,
    extraTrafficConsumed = extraTrafficConsumed,
    baseTrafficRemainder = baseTrafficRemainder,
  )

  def toTrafficPurchased(member: Member): Option[TrafficPurchased] = serial.map { s =>
    TrafficPurchased(
      member = member,
      sequencingTimestamp = timestamp,
      extraTrafficPurchased = extraTrafficPurchased,
      serial = s,
    )
  }

  override def pretty: Pretty[TrafficState] = prettyOfClass(
    param("extraTrafficLimit", _.extraTrafficPurchased),
    param("extraTrafficConsumed", _.extraTrafficConsumed),
    param("baseTrafficRemainder", _.baseTrafficRemainder),
    param("lastConsumedCost", _.lastConsumedCost),
    param("timestamp", _.timestamp),
    paramIfDefined("serial", _.serial),
  )
}

object TrafficState {

  implicit val setResultParameter: SetParameter[TrafficState] = { (v: TrafficState, pp) =>
    pp >> Some(v.extraTrafficPurchased.value)
    pp >> Some(v.extraTrafficConsumed.value)
    pp >> Some(v.baseTrafficRemainder.value)
    pp >> Some(v.lastConsumedCost.value)
    pp >> v.timestamp
    pp >> v.serial.map(_.value)
  }

  implicit val getResultTrafficState: GetResult[Option[TrafficState]] = {
    GetResult
      .createGetTuple6(
        nonNegativeLongOptionGetResult,
        nonNegativeLongOptionGetResult,
        nonNegativeLongOptionGetResult,
        nonNegativeLongOptionGetResult,
        CantonTimestamp.getResultOptionTimestamp,
        GetResult(_ => Some(Option.empty[PositiveInt])),
      )
      .andThen(_.mapN(TrafficState.apply))
  }

  def empty: TrafficState = TrafficState(
    NonNegativeLong.zero,
    NonNegativeLong.zero,
    NonNegativeLong.zero,
    NonNegativeLong.zero,
    CantonTimestamp.Epoch,
    Option.empty,
  )

  def empty(timestamp: CantonTimestamp): TrafficState = TrafficState(
    NonNegativeLong.zero,
    NonNegativeLong.zero,
    NonNegativeLong.zero,
    NonNegativeLong.zero,
    timestamp,
    Option.empty,
  )

  def fromProtoV30(
      trafficStateP: v30.TrafficState
  ): Either[ProtoDeserializationError, TrafficState] = for {
    extraTrafficLimit <- ProtoConverter.parseNonNegativeLong(trafficStateP.extraTrafficPurchased)
    extraTrafficConsumed <- ProtoConverter.parseNonNegativeLong(trafficStateP.extraTrafficConsumed)
    baseTrafficRemainder <- ProtoConverter.parseNonNegativeLong(trafficStateP.baseTrafficRemainder)
    lastConsumedCost <- ProtoConverter.parseNonNegativeLong(trafficStateP.lastConsumedCost)
    timestamp <- CantonTimestamp.fromProtoPrimitive(trafficStateP.timestamp)
    serial <- trafficStateP.serial.traverse(ProtoConverter.parsePositiveInt)
  } yield TrafficState(
    extraTrafficLimit,
    extraTrafficConsumed,
    baseTrafficRemainder,
    lastConsumedCost,
    timestamp,
    serial,
  )
}
