// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.daml.metrics.api.MetricName
import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitError.AboveTrafficLimit
import com.digitalasset.canton.metrics.CantonLabeledMetricsFactory.NoOpMetricsFactory
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.traffic.{EventCostCalculator, TopUpEvent}
import com.digitalasset.canton.{BaseTest, metrics, time}
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec

class SequencerMemberRateLimiterTest extends AnyFlatSpec with BaseTest {
  behavior of "sequencer rate limiter"

  private val recipient1 = mock[Member]
  private val recipient2 = mock[Member]

  // Doesn't matter as the event cost function is mocked later
  private val mockEvent = makeBatch(
    List(
      ClosedEnvelope.create(
        ByteString.copyFrom(Array.fill(5)(1.toByte)),
        Recipients.cc(recipient1, recipient2),
        Seq.empty,
        testedProtocolVersion,
      )
    )
  )

  private val trafficControlConfig = TrafficControlParameters(
    NonNegativeNumeric.tryCreate(200L),
    PositiveNumeric.tryCreate(200),
    time.NonNegativeFiniteDuration.tryOfSeconds(10),
  )

  private val start = CantonTimestamp.now()
  private val sender = mock[Member]
  private val eventCostCalculator = mock[EventCostCalculator]
  private val sequencerMetrics = new SequencerMetrics(
    MetricName("test"),
    NoOpMetricsFactory,
    Metrics.ForTesting.grpc,
    metrics.Metrics.ForTesting.health,
  )

  private def makeBatch(envelopes: List[ClosedEnvelope]) = {
    Batch.apply(envelopes, testedProtocolVersion)
  }

  private def makeLimiter = {
    new SequencerMemberRateLimiter(
      sender,
      Seq.empty,
      loggerFactory,
      sequencerMetrics,
      eventCostCalculator,
    )
  }

  it should "consume event if enough base rate" in {
    val rateLimiter =
      new SequencerMemberRateLimiter(
        sender,
        Seq.empty,
        loggerFactory,
        sequencerMetrics,
        eventCostCalculator,
      )

    when(
      eventCostCalculator.computeEventCost(
        any[Batch[ClosedEnvelope]],
        any[PositiveNumeric[Int]],
        any[Map[GroupRecipient, Set[Member]]],
      )
    )
      .thenAnswer(NonNegativeLong.tryCreate(40))

    // Base rate = 20 -> 100 bytes allowed after 5 seconds
    val eventTimestamp = start.plusSeconds(5)
    rateLimiter
      .tryConsume(
        mockEvent,
        eventTimestamp,
        trafficControlConfig,
        TrafficState.empty(start),
        Map.empty,
        sender,
      ) shouldBe Right(
      TrafficState(
        extraTrafficRemainder = NonNegativeLong.zero,
        extraTrafficConsumed = NonNegativeLong.zero,
        baseTrafficRemainder = NonNegativeLong.tryCreate(60), // 100 - 40
        timestamp = eventTimestamp,
      )
    ) -> None
  }

  it should "reject if not enough base rate" in {
    val rateLimiter = makeLimiter

    when(
      eventCostCalculator.computeEventCost(
        any[Batch[ClosedEnvelope]],
        any[PositiveNumeric[Int]],
        any[Map[GroupRecipient, Set[Member]]],
      )
    )
      .thenAnswer(NonNegativeLong.tryCreate(21)) // just 1 above base rate

    // Base traffic = 20
    val eventTimestamp = start.plusSeconds(1)
    rateLimiter
      .tryConsume(
        mockEvent,
        eventTimestamp,
        trafficControlConfig,
        TrafficState.empty(start),
        Map.empty,
        sender,
      ) shouldBe Left(
      AboveTrafficLimit(
        sender,
        NonNegativeLong.tryCreate(21),
        TrafficState(
          NonNegativeLong.zero,
          NonNegativeLong.zero,
          NonNegativeLong.tryCreate(20),
          eventTimestamp,
        ),
      )
    ) -> None
  }

  it should "allow top up" in {
    val rateLimiter = makeLimiter

    rateLimiter.topUp(
      TopUpEvent(PositiveLong.tryCreate(50L), start, PositiveInt.tryCreate(1))
    )
    rateLimiter.getTrafficLimit(start).value shouldBe 50L
  }

  it should "consume only from base rate if enough" in {
    val rateLimiter = makeLimiter

    val topUp =
      TopUpEvent(PositiveLong.tryCreate(50L), start, PositiveInt.tryCreate(1))
    rateLimiter.topUp(topUp)
    rateLimiter.getTrafficLimit(start).value shouldBe 50L

    when(
      eventCostCalculator.computeEventCost(
        any[Batch[ClosedEnvelope]],
        any[PositiveNumeric[Int]],
        any[Map[GroupRecipient, Set[Member]]],
      )
    )
      .thenAnswer(NonNegativeLong.tryCreate(40))

    // Base rate = 20 -> 100 bytes allowed after 5 seconds
    // Extra limit = 50
    val eventTimestamp = start.plusSeconds(5)
    rateLimiter
      .tryConsume(
        mockEvent,
        eventTimestamp,
        trafficControlConfig,
        TrafficState.empty(start),
        Map.empty,
        sender,
      ) shouldBe Right(
      TrafficState(
        extraTrafficRemainder = NonNegativeLong.tryCreate(50L),
        extraTrafficConsumed = NonNegativeLong.zero,
        baseTrafficRemainder = NonNegativeLong.tryCreate(60), // 100 - 40
        timestamp = eventTimestamp,
      )
    ) -> Some(topUp)
  }

  it should "consume from base rate and extra limit" in {
    val rateLimiter = makeLimiter

    rateLimiter.topUp(
      TopUpEvent(PositiveLong.tryCreate(50L), start, PositiveInt.tryCreate(1))
    )
    rateLimiter.getTrafficLimit(start).value shouldBe 50L

    when(
      eventCostCalculator.computeEventCost(
        any[Batch[ClosedEnvelope]],
        any[PositiveNumeric[Int]],
        any[Map[GroupRecipient, Set[Member]]],
      )
    )
      .thenAnswer(NonNegativeLong.tryCreate(30))

    // Base rate = 20 bytes allowed after 1 second
    // Extra limit = 50
    val eventTimestamp = start.plusSeconds(1)
    val trafficState1 = rateLimiter
      .tryConsume(
        mockEvent,
        eventTimestamp,
        trafficControlConfig,
        TrafficState.empty(start),
        Map.empty,
        sender,
      )
      ._1
      .value
    trafficState1 shouldBe TrafficState(
      extraTrafficRemainder = NonNegativeLong.tryCreate(40L), // Traffic limit (50) - consumed (10)
      extraTrafficConsumed =
        NonNegativeLong.tryCreate(10L), // because event cost is 30 - 20 from base rate
      baseTrafficRemainder = NonNegativeLong.zero,
      timestamp = eventTimestamp,
    )

    val eventTimestamp2 = eventTimestamp.plusMillis(1)
    rateLimiter
      .tryConsume(
        mockEvent,
        eventTimestamp2,
        trafficControlConfig,
        trafficState1,
        Map.empty,
        sender,
      ) shouldBe Right(
      TrafficState(
        extraTrafficRemainder =
          NonNegativeLong.tryCreate(10L), // Traffic limit (50) - consumed (40)
        extraTrafficConsumed = NonNegativeLong.tryCreate(
          40L
        ), // consume full amount from extra limit as no base rate was accumulated
        baseTrafficRemainder = NonNegativeLong.zero,
        timestamp = eventTimestamp2,
      )
    ) -> None
  }

  it should "reject if not enough even with extra limit" in {
    val rateLimiter = makeLimiter

    val topUp =
      TopUpEvent(PositiveLong.tryCreate(50L), start, PositiveInt.tryCreate(1))
    rateLimiter.topUp(topUp)
    rateLimiter.getTrafficLimit(start).value shouldBe 50L

    when(
      eventCostCalculator.computeEventCost(
        any[Batch[ClosedEnvelope]],
        any[PositiveNumeric[Int]],
        any[Map[GroupRecipient, Set[Member]]],
      )
    )
      .thenAnswer(NonNegativeLong.tryCreate(200))

    // Base rate = 20 -> 100 bytes allowed after 5 seconds
    // Extra limit = 50
    val eventTimestamp = start.plusSeconds(5)
    rateLimiter
      .tryConsume(
        mockEvent,
        eventTimestamp,
        trafficControlConfig,
        TrafficState.empty(start),
        Map.empty,
        sender,
      ) shouldBe Left(
      AboveTrafficLimit(
        sender,
        NonNegativeLong.tryCreate(200L),
        TrafficState(
          extraTrafficRemainder = NonNegativeLong.tryCreate(50L),
          extraTrafficConsumed = NonNegativeLong.zero,
          baseTrafficRemainder = NonNegativeLong.tryCreate(100L),
          timestamp = eventTimestamp,
        ),
      )
    ) -> Some(topUp)
  }
}
