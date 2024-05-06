// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficPurchasedStore
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, HasExecutionContext}

trait RateLimitManagerTesting { this: BaseTest with HasExecutionContext =>
  lazy val trafficPurchasedStore = new InMemoryTrafficPurchasedStore(loggerFactory)
  def mkTrafficPurchasedManager(store: TrafficPurchasedStore) = new TrafficPurchasedManager(
    store,
    new SimClock(CantonTimestamp.Epoch, loggerFactory),
    SequencerTrafficConfig(),
    futureSupervisor,
    SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
    testedProtocolVersion,
    timeouts,
    loggerFactory,
  )
  lazy val defaultTrafficPurchasedManager = mkTrafficPurchasedManager(trafficPurchasedStore)

  lazy val defaultRateLimiter = mkRateLimiter(trafficPurchasedStore)
  def defaultRateLimiterWithEventCostCalculator(eventCostCalculator: EventCostCalculator) =
    new EnterpriseSequencerRateLimitManager(
      defaultTrafficPurchasedManager,
      loggerFactory,
      futureSupervisor,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      eventCostCalculator = eventCostCalculator,
      protocolVersion = testedProtocolVersion,
    )

  def mkRateLimiter(store: TrafficPurchasedStore) =
    new EnterpriseSequencerRateLimitManager(
      mkTrafficPurchasedManager(store),
      loggerFactory,
      futureSupervisor,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      eventCostCalculator = new EventCostCalculator(loggerFactory),
      protocolVersion = testedProtocolVersion,
    )

  def mkRateLimiter(manager: TrafficPurchasedManager) =
    new EnterpriseSequencerRateLimitManager(
      manager,
      loggerFactory,
      futureSupervisor,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      eventCostCalculator = new EventCostCalculator(loggerFactory),
      protocolVersion = testedProtocolVersion,
    )
}
