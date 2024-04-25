// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfDomain, ClosedEnvelope, Recipients}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAnyWordSpec}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class EventCostCalculatorTest
    extends AnyWordSpec
    with BaseTest
    with ProtocolVersionChecksAnyWordSpec {
  private val recipient1 = mock[Member]
  private val recipient2 = mock[Member]

  "calculate cost correctly" in {
    new EventCostCalculator().computeEnvelopeCost(
      PositiveInt.tryCreate(5000),
      Map.empty,
    )(
      ClosedEnvelope.create(
        ByteString.copyFrom(Array.fill(5)(1.toByte)),
        Recipients.cc(recipient1, recipient2),
        Seq.empty,
        testedProtocolVersion,
      )
    ) shouldBe 10L // == 5 + 5 * 2 * 5000 / 10000
  }

  "use resolved group recipients" in {
    new EventCostCalculator().computeEnvelopeCost(
      PositiveInt.tryCreate(5000),
      Map(AllMembersOfDomain -> Set(recipient1, recipient2)),
    )(
      ClosedEnvelope.create(
        ByteString.copyFrom(Array.fill(5)(1.toByte)),
        Recipients.cc(AllMembersOfDomain),
        Seq.empty,
        testedProtocolVersion,
      )
    ) shouldBe 10L // == 5 + 5 * 2 * 5000 / 10000
  }
}
