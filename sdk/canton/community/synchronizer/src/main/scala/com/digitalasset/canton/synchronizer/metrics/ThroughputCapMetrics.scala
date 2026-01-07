// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}

class ThroughputCapMetrics(
    messageName: String,
    parent: MetricName,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends AutoCloseable {
  private val prefix: MetricName = parent :+ "throughput-cap" :+ messageName.replace(" ", "-")

  val tps: Gauge[Double] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        name = prefix :+ "tps",
        summary = "Global transactions per second",
        description =
          """Current global transactions per second computed based on a configurable window""".stripMargin,
        qualification = MetricQualification.Traffic,
      ),
      0d,
    )(MetricsContext.Empty)

  val bps: Gauge[Double] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        name = prefix :+ "bps",
        summary = "Global bytes per second",
        description =
          """Current global bytes per second computed based on a configurable window""".stripMargin,
        qualification = MetricQualification.Traffic,
      ),
      0d,
    )(MetricsContext.Empty)

  override def close(): Unit = {
    tps.close()
    bps.close()
  }
}
