// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.config

import com.daml.metrics.api.reporters.MetricsReporter

final case class MetricsConfig(
    reporter: MetricsReporter = MetricsReporter.None
) {
  val enabled: Boolean = reporter == MetricsReporter.None
}

object MetricsConfig {
  sealed trait MetricRegistryType

  val DefaultMetricsConfig: MetricsConfig = MetricsConfig()
}
