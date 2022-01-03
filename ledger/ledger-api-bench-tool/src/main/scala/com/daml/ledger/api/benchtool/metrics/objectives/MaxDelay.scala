// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics.objectives

import com.daml.ledger.api.benchtool.metrics.DelayMetric

final case class MaxDelay(maxDelaySeconds: Long) extends ServiceLevelObjective[DelayMetric.Value] {
  override def isViolatedBy(metricValue: DelayMetric.Value): Boolean =
    metricValue.meanDelaySeconds.exists(_ > maxDelaySeconds)
}
