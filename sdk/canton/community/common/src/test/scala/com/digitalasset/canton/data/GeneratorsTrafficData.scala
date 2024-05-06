// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.protocol.messages.SetTrafficPurchasedMessage
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalacheck.Arbitrary

final class GeneratorsTrafficData(
    protocolVersion: ProtocolVersion
) {
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*

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
}
