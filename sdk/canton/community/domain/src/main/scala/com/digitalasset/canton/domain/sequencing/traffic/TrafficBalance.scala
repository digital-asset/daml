// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencer.admin.v30.SequencerSnapshot.TrafficBalance as TrafficBalanceP
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import slick.jdbc.{GetResult, SetParameter}

/** Traffic balance for a member valid at a specific timestamp
  *
  * @param member              Member to which the balance belongs
  * @param serial              Serial number of the balance
  * @param balance             Balance value
  * @param sequencingTimestamp Timestamp at which the balance was sequenced
  */
final case class TrafficBalance(
    member: Member,
    serial: PositiveInt,
    balance: NonNegativeLong,
    sequencingTimestamp: CantonTimestamp,
) extends PrettyPrinting {
  override def pretty: Pretty[TrafficBalance] =
    prettyOfClass(
      param("member", _.member),
      param("sequencingTimestamp", _.sequencingTimestamp),
      param("balance", _.balance),
      param("serial", _.serial),
    )

  def toProtoV30: TrafficBalanceP = {
    TrafficBalanceP(
      member = member.toProtoPrimitive,
      serial = serial.value,
      balance = balance.value,
      sequencingTimestamp = sequencingTimestamp.toProtoPrimitive,
    )
  }
}

object TrafficBalance {
  import com.digitalasset.canton.store.db.RequiredTypesCodec.*
  import com.digitalasset.canton.topology.Member.DbStorageImplicits.*

  implicit val trafficBalanceOrdering: Ordering[TrafficBalance] =
    Ordering.by(_.sequencingTimestamp)

  implicit val trafficBalanceGetResult: GetResult[TrafficBalance] =
    GetResult.createGetTuple4[Member, CantonTimestamp, NonNegativeLong, PositiveInt].andThen {
      case (member, ts, balance, serial) => TrafficBalance(member, serial, balance, ts)
    }

  implicit val trafficBalanceSetParameter: SetParameter[TrafficBalance] =
    SetParameter[TrafficBalance] { (balance, pp) =>
      pp >> balance.member
      pp >> balance.sequencingTimestamp
      pp >> balance.balance
      pp >> balance.serial
    }

  def fromProtoV30(trafficBalanceP: TrafficBalanceP): ParsingResult[TrafficBalance] =
    for {
      member <- Member.fromProtoPrimitive(trafficBalanceP.member, "member")
      serial <- ProtoConverter.parsePositiveInt(trafficBalanceP.serial)
      balance <- ProtoConverter.parseNonNegativeLong(trafficBalanceP.balance)
      sequencingTimestamp <- CantonTimestamp.fromProtoPrimitive(
        trafficBalanceP.sequencingTimestamp
      )
    } yield TrafficBalance(
      member = member,
      serial = serial,
      balance = balance,
      sequencingTimestamp = sequencingTimestamp,
    )
}
