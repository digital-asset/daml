// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  Batch,
  ClosedEnvelope,
  Recipients,
}
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator.{
  EnvelopeCostDetails,
  EventCostDetails,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAnyWordSpec}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class EventCostCalculatorTest
    extends AnyWordSpec
    with BaseTest
    with ProtocolVersionChecksAnyWordSpec {
  private val recipient1 = DefaultTestIdentities.participant1
  private val recipient2 = DefaultTestIdentities.participant2

  "calculate cost correctly" in {
    val recipients = Recipients.cc(recipient1, recipient2)
    new EventCostCalculator(loggerFactory).computeEnvelopeCost(
      PositiveInt.tryCreate(5000),
      Map.empty,
    )(
      ClosedEnvelope.create(
        ByteString.copyFrom(Array.fill(5)(1.toByte)),
        recipients,
        Seq.empty,
        testedProtocolVersion,
      )
    ) shouldBe EnvelopeCostDetails(
      writeCost = 5L,
      readCost = 5L, // 5 * 2 * 5000 / 10000
      finalCost = 10L,
      recipients = recipients.allRecipients.toSeq,
    )
  }

  "use resolved group recipients" in {
    val recipients = Recipients.cc(AllMembersOfSynchronizer)
    new EventCostCalculator(loggerFactory).computeEnvelopeCost(
      PositiveInt.tryCreate(5000),
      Map(AllMembersOfSynchronizer -> Set(recipient1, recipient2)),
    )(
      ClosedEnvelope.create(
        ByteString.copyFrom(Array.fill(5)(1.toByte)),
        recipients,
        Seq.empty,
        testedProtocolVersion,
      )
    ) shouldBe EnvelopeCostDetails(
      writeCost = 5L,
      readCost = 5L, // 5 * 2 * 5000 / 10000
      finalCost = 10L,
      recipients.allRecipients.toSeq,
    )
  }

  "cost computation does not overflow an int" in {
    // Trying to reproduce case seen on CN devnet:
    // ~ 500 recipients, cost multiplier 200, estimated payload 25000
    // This overflows an Int computation (-154496 instead of 275000)

    val recipients = Recipients.cc(AllMembersOfSynchronizer)
    val manyRecipients = List.fill(500)(mock[Member]).toSet
    new EventCostCalculator(loggerFactory).computeEnvelopeCost(
      PositiveInt.tryCreate(200),
      Map(AllMembersOfSynchronizer -> manyRecipients),
    )(
      ClosedEnvelope.create(
        ByteString.copyFrom(Array.fill(25000)(1.toByte)),
        recipients,
        Seq.empty,
        testedProtocolVersion,
      )
    ) shouldBe EnvelopeCostDetails(
      writeCost = 25000L,
      readCost = 250000L, // 25000 * 500 * 200 / 10000
      finalCost = 275000L,
      recipients.allRecipients.toSeq,
    )
  }

  "detect cost computation overflow" in {
    val manyRecipients = List.fill(1_000)(mock[Member]).toSet

    val exception = intercept[IllegalStateException](
      new EventCostCalculator(loggerFactory).computeEnvelopeCost(
        PositiveInt.tryCreate(1_000_000_000),
        Map(AllMembersOfSynchronizer -> manyRecipients),
      )(
        ClosedEnvelope.create(
          ByteString.copyFrom(Array.fill(10_000_000)(1.toByte)),
          Recipients.cc(AllMembersOfSynchronizer),
          Seq.empty,
          testedProtocolVersion,
        )
      )
    )

    exception.getMessage should include("Overflow in cost computation")
  }

  "respect minimum event cost" in {
    val recipients = Recipients.cc(recipient1, recipient2)
    val expectedEnvelopeCost = EnvelopeCostDetails(
      writeCost = 5L,
      readCost = 5L, // 5 * 2 * 5000 / 10000
      finalCost = 10L,
      recipients = recipients.allRecipients.toSeq,
    )
    val baseCost = NonNegativeLong.tryCreate(350)
    new EventCostCalculator(loggerFactory).computeEventCost(
      Batch.fromClosed(
        testedProtocolVersion,
        ClosedEnvelope.create(
          ByteString.copyFrom(Array.fill(5)(1.toByte)),
          recipients,
          Seq.empty,
          testedProtocolVersion,
        ),
      ),
      PositiveInt.tryCreate(5000),
      Map.empty,
      testedProtocolVersion,
      baseEventCost = baseCost,
    ) shouldBe EventCostDetails(
      costMultiplier = PositiveInt.tryCreate(5000),
      groupToMembersSize = Map.empty,
      envelopes = List(expectedEnvelopeCost),
      eventCost = NonNegativeLong.tryCreate(expectedEnvelopeCost.finalCost + baseCost.value),
    )
  }
}
