// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import io.opentelemetry.sdk.metrics.data.MetricData

final case class MetricsSnapshot(
    otelMetrics: Seq[MetricData]
) {
  def toMap: Map[String, Seq[MetricValue]] = otelMetrics.map { value =>
    (value.getName, MetricValue.fromMetricData(value))
  }.toMap
}

object MetricsSnapshot {

  def apply(reader: OnDemandMetricsReader): MetricsSnapshot = {
    MetricsSnapshot(
      otelMetrics = reader.read()
    )
  }
}
