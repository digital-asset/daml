// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.traffic.{EventCostCalculator, TopUpEvent}

trait SequencerMemberRateLimiterFactory {
  def create(
      member: Member,
      topUps: Seq[TopUpEvent],
      loggerFactory: NamedLoggerFactory,
      metrics: SequencerMetrics,
      eventCostCalculator: EventCostCalculator,
  ): SequencerMemberRateLimiter
}

object DefaultSequencerMemberRateLimiterFactory extends SequencerMemberRateLimiterFactory {
  override def create(
      member: Member,
      topUps: Seq[TopUpEvent],
      loggerFactory: NamedLoggerFactory,
      metrics: SequencerMetrics,
      eventCostCalculator: EventCostCalculator,
  ): SequencerMemberRateLimiter = new SequencerMemberRateLimiter(
    member,
    topUps,
    loggerFactory,
    metrics,
    eventCostCalculator,
  )
}
