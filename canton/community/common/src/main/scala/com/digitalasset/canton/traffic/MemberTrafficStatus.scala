// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.traffic.v30.MemberTrafficStatus as MemberTrafficStatusP
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.SequencedEventTrafficState
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.Member

final case class MemberTrafficStatus(
    member: Member,
    timestamp: CantonTimestamp,
    trafficState: SequencedEventTrafficState,
    balanceSerial: Option[PositiveInt],
) extends Product
    with PrettyPrinting {
  def toProtoV30: MemberTrafficStatusP = {
    MemberTrafficStatusP(
      member.toProtoPrimitive,
      trafficState.extraTrafficLimit.map(_.value),
      trafficState.extraTrafficConsumed.value,
      List.empty,
      Some(timestamp.toProtoTimestamp),
      balanceSerial.map(_.value),
    )
  }
  override def pretty: Pretty[this.type] = adHocPrettyInstance
}

object MemberTrafficStatus {
  def fromProtoV30(
      trafficStatusP: MemberTrafficStatusP
  ): Either[ProtoDeserializationError, MemberTrafficStatus] = {
    for {
      member <- Member.fromProtoPrimitive(
        trafficStatusP.member,
        "member",
      )
      totalExtraTrafficLimitOpt <- trafficStatusP.totalExtraTrafficLimit.traverse(
        ProtoConverter.parseNonNegativeLong
      )
      balanceSerialOpt <- trafficStatusP.balanceSerial.traverse(
        ProtoConverter.parsePositiveInt
      )
      totalExtraTrafficConsumed <- ProtoConverter.parseNonNegativeLong(
        trafficStatusP.totalExtraTrafficConsumed
      )
      totalExtraTrafficRemainder <- ProtoConverter.parseNonNegativeLong(
        totalExtraTrafficLimitOpt.map(_.value - totalExtraTrafficConsumed.value).getOrElse(0L)
      )
      ts <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "ts",
        trafficStatusP.ts,
      )
    } yield MemberTrafficStatus(
      member,
      ts,
      SequencedEventTrafficState(
        totalExtraTrafficRemainder,
        totalExtraTrafficConsumed,
      ),
      balanceSerialOpt,
    )
  }
}
