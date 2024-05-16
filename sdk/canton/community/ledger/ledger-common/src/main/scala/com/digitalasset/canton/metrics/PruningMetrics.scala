// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}

class PruningMetrics(prefix: MetricName, factory: LabeledMetricsFactory) {
  import com.daml.metrics.api.MetricsContext.Implicits.empty
  // Using a meter, which can keep track of how many times the operation was executed, even if the
  // operation is fully executed between 2 metric fetches by the monitoring.
  // With a (boolean) gauge, there is a large risk that some operation invocation would be missed.
  val pruneCommandStarted =
    factory.meter(
      MetricInfo(
        prefix :+ "prune" :+ "started",
        "Total number of started pruning processes.",
        MetricQualification.Debug,
      )
    )
  val pruneCommandCompleted =
    factory.meter(
      MetricInfo(
        prefix :+ "prune" :+ "completed",
        "Total number of completed pruning processes.",
        MetricQualification.Debug,
      )
    )
}
