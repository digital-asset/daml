// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics.objectives

import com.daml.ledger.api.benchtool.metrics.CountRateMetric

final case class MinRate(minAllowedRatePerSecond: Double)
    extends ServiceLevelObjective[CountRateMetric.Value] {
  override def isViolatedBy(metricValue: CountRateMetric.Value): Boolean =
    Ordering[CountRateMetric.Value].lt(metricValue, v)

  private val v = CountRateMetric.Value(minAllowedRatePerSecond)
}
