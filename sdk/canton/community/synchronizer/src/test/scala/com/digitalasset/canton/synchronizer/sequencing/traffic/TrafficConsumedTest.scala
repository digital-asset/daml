// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.topology.ParticipantId
import org.scalatest.wordspec.AnyWordSpec

class TrafficConsumedTest extends AnyWordSpec with BaseTest {

  val alice = ParticipantId("alice")
  val params = TrafficControlParameters(maxBaseTrafficAmount = NonNegativeLong.maxValue)

  "TrafficConsumed" should {
    "handle correctly overflowing base rate accumulation" in {
      val ts = CantonTimestamp.now()
      val tc = TrafficConsumed(
        alice.member,
        extraTrafficConsumed = NonNegativeLong.zero,
        baseTrafficRemainder = NonNegativeLong.maxValue,
        lastConsumedCost = NonNegativeLong.zero,
        sequencingTimestamp = ts,
      )
      // The base traffic is already maxed out, and calling compute with a more recent timestamp would overflow the Long
      // that holds base traffic
      val consumed = tc.consume(
        ts.plusSeconds(200),
        params,
        NonNegativeLong.zero,
        logger,
      )
      // Make sure we don't overflow and stay at max value
      consumed.baseTrafficRemainder shouldBe NonNegativeLong.maxValue
    }
  }
}
