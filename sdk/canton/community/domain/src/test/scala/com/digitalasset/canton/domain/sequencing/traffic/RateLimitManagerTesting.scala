// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficBalanceStore
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficBalanceStore
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.traffic.EventCostCalculator
import com.digitalasset.canton.{BaseTest, HasExecutionContext}

trait RateLimitManagerTesting { this: BaseTest with HasExecutionContext =>
  lazy val trafficBalanceStore = new InMemoryTrafficBalanceStore(loggerFactory)
  def mkTrafficBalanceManager(store: TrafficBalanceStore) = new TrafficBalanceManager(
    store,
    new SimClock(CantonTimestamp.Epoch, loggerFactory),
    SequencerTrafficConfig(),
    futureSupervisor,
    SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
    timeouts,
    loggerFactory,
  )
  lazy val defaultTrafficBalanceManager = mkTrafficBalanceManager(trafficBalanceStore)

  lazy val defaultRateLimiter = mkRateLimiter(trafficBalanceStore)
  def defaultRateLimiterWithEventCostCalculator(eventCostCalculator: EventCostCalculator) =
    new EnterpriseSequencerRateLimitManager(
      defaultTrafficBalanceManager,
      loggerFactory,
      futureSupervisor,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      eventCostCalculator = eventCostCalculator,
    )

  def mkRateLimiter(store: TrafficBalanceStore) =
    new EnterpriseSequencerRateLimitManager(
      mkTrafficBalanceManager(store),
      loggerFactory,
      futureSupervisor,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      eventCostCalculator = new EventCostCalculator(loggerFactory),
    )

  def mkRateLimiter(manager: TrafficBalanceManager) =
    new EnterpriseSequencerRateLimitManager(
      manager,
      loggerFactory,
      futureSupervisor,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      eventCostCalculator = new EventCostCalculator(loggerFactory),
    )
}
