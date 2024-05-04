// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30.TrafficPurchased as TrafficPurchasedP
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import slick.jdbc.{GetResult, SetParameter}

/** Total traffic purchased for a member valid at a specific timestamp
  *
  * @param member              Member to which the balance belongs
  * @param serial              Serial number of the balance
  * @param extraTrafficPurchased             Traffic purchased value
  * @param sequencingTimestamp Timestamp at which the purchase event was sequenced
  */
final case class TrafficPurchased(
    member: Member,
    serial: PositiveInt,
    extraTrafficPurchased: NonNegativeLong,
    sequencingTimestamp: CantonTimestamp,
) extends PrettyPrinting {
  override def pretty: Pretty[TrafficPurchased] =
    prettyOfClass(
      param("member", _.member),
      param("sequencingTimestamp", _.sequencingTimestamp),
      param("extraTrafficPurchased", _.extraTrafficPurchased),
      param("serial", _.serial),
    )

  def toProtoV30: TrafficPurchasedP = {
    TrafficPurchasedP(
      member = member.toProtoPrimitive,
      serial = serial.value,
      extraTrafficPurchased = extraTrafficPurchased.value,
      sequencingTimestamp = sequencingTimestamp.toProtoPrimitive,
    )
  }
}

object TrafficPurchased {
  import com.digitalasset.canton.store.db.RequiredTypesCodec.*
  import com.digitalasset.canton.topology.Member.DbStorageImplicits.*

  implicit val trafficPurchasedOrdering: Ordering[TrafficPurchased] =
    Ordering.by(_.sequencingTimestamp)

  implicit val trafficPurchasedGetResult: GetResult[TrafficPurchased] =
    GetResult.createGetTuple4[Member, CantonTimestamp, NonNegativeLong, PositiveInt].andThen {
      case (member, ts, balance, serial) => TrafficPurchased(member, serial, balance, ts)
    }

  implicit val trafficPurchasedSetParameter: SetParameter[TrafficPurchased] =
    SetParameter[TrafficPurchased] { (balance, pp) =>
      pp >> balance.member
      pp >> balance.sequencingTimestamp
      pp >> balance.extraTrafficPurchased
      pp >> balance.serial
    }

  def fromProtoV30(trafficPurchasedP: TrafficPurchasedP): ParsingResult[TrafficPurchased] =
    for {
      member <- Member.fromProtoPrimitive(trafficPurchasedP.member, "member")
      serial <- ProtoConverter.parsePositiveInt(trafficPurchasedP.serial)
      balance <- ProtoConverter.parseNonNegativeLong(trafficPurchasedP.extraTrafficPurchased)
      sequencingTimestamp <- CantonTimestamp.fromProtoPrimitive(
        trafficPurchasedP.sequencingTimestamp
      )
    } yield TrafficPurchased(
      member = member,
      serial = serial,
      extraTrafficPurchased = balance,
      sequencingTimestamp = sequencingTimestamp,
    )
}
