// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.metrics.MetricHandle.LabeledMetricsFactory

sealed trait MetricsFactoryType

object MetricsFactoryType {

  // Used to provide an in-memory metrics factory for testing
  // Most provide a new instance for each component
  final case class InMemory(provider: MetricsContext => LabeledMetricsFactory)
      extends MetricsFactoryType
  // Use actual Dropwizard/OpenTelemetry implementations
  case object External extends MetricsFactoryType
}
