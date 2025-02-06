// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricName, MetricQualification}

// Private constructor to avoid being instantiated multiple times by accident
class PruningMetrics private[metrics] (prefix: MetricName, factory: LabeledMetricsFactory) {
  import com.daml.metrics.api.MetricsContext.Implicits.empty

  // Using a meter, which can keep track of how many times the operation was executed, even if the
  // operation is fully executed between 2 metric fetches by the monitoring.
  // With a (boolean) gauge, there is a large risk that some operation invocation would be missed.
  val pruneCommandStarted: MetricHandle.Meter =
    factory.meter(
      MetricInfo(
        prefix :+ "prune" :+ "started",
        "Total number of started pruning processes.",
        MetricQualification.Debug,
      )
    )
  val pruneCommandCompleted: MetricHandle.Meter =
    factory.meter(
      MetricInfo(
        prefix :+ "prune" :+ "completed",
        "Total number of completed pruning processes.",
        MetricQualification.Debug,
      )
    )
}
