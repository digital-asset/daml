// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics.objectives

import com.daml.ledger.api.benchtool.metrics.ConsumptionSpeedMetric

// TODO: add warm-up parameter
final case class MinConsumptionSpeed(minSpeed: Double)
    extends ServiceLevelObjective[ConsumptionSpeedMetric.Value] {
  override def isViolatedBy(metricValue: ConsumptionSpeedMetric.Value): Boolean =
    Ordering[ConsumptionSpeedMetric.Value].lt(metricValue, v)

  private val v = ConsumptionSpeedMetric.Value(Some(minSpeed))
}
