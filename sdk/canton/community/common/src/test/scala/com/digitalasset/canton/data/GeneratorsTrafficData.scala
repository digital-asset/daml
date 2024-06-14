// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.protocol.messages.SetTrafficPurchasedMessage
import com.digitalasset.canton.sequencing.protocol.{
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  TrafficState,
}
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalacheck.Arbitrary

final class GeneratorsTrafficData(
    protocolVersion: ProtocolVersion
) {
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import GeneratorsDataTime.*

  implicit val setTrafficPurchasedArb: Arbitrary[SetTrafficPurchasedMessage] = Arbitrary(
    for {
      member <- Arbitrary.arbitrary[Member]
      serial <- Arbitrary.arbitrary[PositiveInt]
      trafficPurchased <- Arbitrary.arbitrary[NonNegativeLong]
      domainId <- Arbitrary.arbitrary[DomainId]
    } yield SetTrafficPurchasedMessage.apply(
      member,
      serial,
      trafficPurchased,
      domainId,
      protocolVersion,
    )
  )

  implicit val getTrafficStateForMemberRequestArb: Arbitrary[GetTrafficStateForMemberRequest] =
    Arbitrary(
      for {
        member <- Arbitrary.arbitrary[Member]
        timestamp <- Arbitrary.arbitrary[CantonTimestamp]
      } yield GetTrafficStateForMemberRequest.apply(
        member,
        timestamp,
        protocolVersion,
      )
    )

  implicit val trafficStateArb: Arbitrary[TrafficState] = Arbitrary(
    for {
      extraTrafficLimit <- Arbitrary.arbitrary[NonNegativeLong]
      extraTrafficConsumed <- Arbitrary.arbitrary[NonNegativeLong]
      baseTrafficRemainder <- Arbitrary.arbitrary[NonNegativeLong]
      timestamp <- Arbitrary.arbitrary[CantonTimestamp]
      serial <- Arbitrary.arbitrary[Option[PositiveInt]]
    } yield TrafficState(
      extraTrafficLimit,
      extraTrafficConsumed,
      baseTrafficRemainder,
      timestamp,
      serial,
    )
  )

  implicit val getTrafficStateForMemberResponseArb: Arbitrary[GetTrafficStateForMemberResponse] =
    Arbitrary(
      for {
        trafficState <- Arbitrary.arbOption[TrafficState].arbitrary
      } yield GetTrafficStateForMemberResponse.apply(
        trafficState,
        protocolVersion,
      )
    )
}
