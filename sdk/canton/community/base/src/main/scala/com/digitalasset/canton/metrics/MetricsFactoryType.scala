// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricsContext

trait MetricsFactoryProvider {
  def generateMetricsFactory(context: MetricsContext): LabeledMetricsFactory
}

sealed trait MetricsFactoryType

object MetricsFactoryType {

  // Used to provide an in-memory metrics factory for testing
  // Most provide a new instance for each component.
  final case class InMemory(provider: MetricsFactoryProvider) extends MetricsFactoryType
  // Use actual OpenTelemetry implementations
  case object External extends MetricsFactoryType
}
