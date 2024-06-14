// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.{
  InMemoryTrafficConsumedStore,
  InMemoryTrafficPurchasedStore,
}
import com.digitalasset.canton.domain.sequencing.traffic.store.{
  TrafficConsumedStore,
  TrafficPurchasedStore,
}
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingTopology}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}

trait RateLimitManagerTesting { this: BaseTest with HasExecutionContext =>
  lazy val trafficPurchasedStore = new InMemoryTrafficPurchasedStore(loggerFactory)
  lazy val trafficConsumedStore = new InMemoryTrafficConsumedStore(loggerFactory)
  lazy val sequencerTrafficConfig = SequencerTrafficConfig()
  def mkTrafficPurchasedManager(store: TrafficPurchasedStore) = new TrafficPurchasedManager(
    store,
    new SimClock(CantonTimestamp.Epoch, loggerFactory),
    sequencerTrafficConfig,
    futureSupervisor,
    SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
    testedProtocolVersion,
    timeouts,
    loggerFactory,
  )
  lazy val defaultTrafficPurchasedManager = mkTrafficPurchasedManager(trafficPurchasedStore)

  lazy val defaultRateLimiter = mkRateLimiter(trafficPurchasedStore)

  lazy val cryptoClient =
    TestingTopology().build(loggerFactory).forOwnerAndDomain(DefaultTestIdentities.participant1)

  def defaultRateLimiterWithEventCostCalculator(eventCostCalculator: EventCostCalculator) =
    new EnterpriseSequencerRateLimitManager(
      defaultTrafficPurchasedManager,
      trafficConsumedStore,
      loggerFactory,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      cryptoClient,
      testedProtocolVersion,
      sequencerTrafficConfig,
      eventCostCalculator = eventCostCalculator,
    )

  def mkRateLimiter(store: TrafficPurchasedStore) =
    new EnterpriseSequencerRateLimitManager(
      mkTrafficPurchasedManager(store),
      trafficConsumedStore,
      loggerFactory,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      cryptoClient,
      testedProtocolVersion,
      sequencerTrafficConfig,
      eventCostCalculator = new EventCostCalculator(loggerFactory),
    )

  def mkRateLimiter(
      manager: TrafficPurchasedManager,
      trafficConsumedStore: TrafficConsumedStore = trafficConsumedStore,
      eventCostCalculator: EventCostCalculator = new EventCostCalculator(loggerFactory),
  ) =
    new EnterpriseSequencerRateLimitManager(
      manager,
      trafficConsumedStore,
      loggerFactory,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      cryptoClient,
      testedProtocolVersion,
      sequencerTrafficConfig,
      eventCostCalculator = eventCostCalculator,
    )
}
