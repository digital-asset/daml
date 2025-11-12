// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

trait ServiceLevelObjective[MetricValueType <: MetricValue] {
  def isViolatedBy(metricValue: MetricValueType): Boolean
}
