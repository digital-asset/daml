// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

trait ServiceLevelObjective[MetricValueType <: MetricValue] {
  def isViolatedBy(metricValue: MetricValueType): Boolean
}
