// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.traffic.v30.MemberTrafficStatus as MemberTrafficStatusP
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.SequencedEventTrafficState
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.Member

final case class MemberTrafficStatus(
    member: Member,
    timestamp: CantonTimestamp,
    trafficState: SequencedEventTrafficState,
    currentAndFutureTopUps: List[TopUpEvent],
) {
  def toProtoV0: MemberTrafficStatusP = {
    MemberTrafficStatusP(
      member.toProtoPrimitive,
      trafficState.extraTrafficLimit.map(_.value),
      trafficState.extraTrafficConsumed.value,
      currentAndFutureTopUps.map(_.toProtoV0),
      Some(timestamp.toProtoPrimitive),
    )
  }
}

object MemberTrafficStatus {
  def fromProtoV0(
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
      totalExtraTrafficConsumed <- ProtoConverter.parseNonNegativeLong(
        trafficStatusP.totalExtraTrafficConsumed
      )
      totalExtraTrafficRemainder <- ProtoConverter.parseNonNegativeLong(
        totalExtraTrafficLimitOpt.map(_.value - totalExtraTrafficConsumed.value).getOrElse(0L)
      )
      topUps <- trafficStatusP.topUpEvents.toList.traverse(TopUpEvent.fromProtoV0)
      ts <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
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
      topUps,
    )
  }
}
