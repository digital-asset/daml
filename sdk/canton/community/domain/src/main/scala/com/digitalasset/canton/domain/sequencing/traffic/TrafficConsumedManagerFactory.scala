// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TrafficConsumptionMetrics
import com.digitalasset.canton.sequencing.traffic.{TrafficConsumed, TrafficConsumedManager}
import com.digitalasset.canton.topology.Member

trait TrafficConsumedManagerFactory {
  def create(
      member: Member,
      lastConsumed: TrafficConsumed,
      loggerFactory: NamedLoggerFactory,
      metrics: TrafficConsumptionMetrics,
  ): TrafficConsumedManager
}

object DefaultTrafficConsumedManagerFactory extends TrafficConsumedManagerFactory {
  override def create(
      member: Member,
      lastConsumed: TrafficConsumed,
      loggerFactory: NamedLoggerFactory,
      metrics: TrafficConsumptionMetrics,
  ): TrafficConsumedManager = {
    new TrafficConsumedManager(
      member,
      lastConsumed,
      loggerFactory,
      metrics,
    )
  }
}
