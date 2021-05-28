// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics.objectives

import com.daml.ledger.api.benchtool.metrics.MetricValue

trait ServiceLevelObjective[MetricValueType <: MetricValue] {
  def isViolatedBy(metricValue: MetricValueType): Boolean
  def formatted: String
}
