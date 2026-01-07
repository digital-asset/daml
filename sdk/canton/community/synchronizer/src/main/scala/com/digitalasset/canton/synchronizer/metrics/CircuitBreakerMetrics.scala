// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.metrics

import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}

class CircuitBreakerMetrics(
    messageName: String,
    parent: MetricName,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends AutoCloseable {
  private val prefix: MetricName = parent :+ "circuit-breaker" :+ messageName.replace(" ", "-")

  val state: Gauge[Double] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        name = prefix :+ "state",
        summary = "Current circuit breaker state",
        description =
          """Current State of the circuit breaker, which could either be Open = 1, HalfOpen = 0.5, Closed = 0""".stripMargin,
        qualification = MetricQualification.Traffic,
      ),
      0d,
    )(MetricsContext.Empty)

  val failures: Counter = openTelemetryMetricsFactory.counter(
    MetricInfo(
      prefix :+ "failures",
      summary = "Count of delayed blocks",
      description =
        s"Count of blocks whose delay was above the allowed block delay for messages of type $messageName",
      qualification = MetricQualification.Traffic,
    )
  )

  val rejections: Counter = openTelemetryMetricsFactory.counter(
    MetricInfo(
      prefix :+ "rejections",
      summary = "Count of rejected messages",
      description =
        s"Count of rejected messages of type $messageName, because the circuit breaker was open",
      qualification = MetricQualification.Traffic,
    )
  )

  val messages: Counter = openTelemetryMetricsFactory.counter(
    MetricInfo(
      prefix :+ "messages",
      summary = "Count of total messages",
      description =
        s"Count of total messages of type $messageName with submission attempted via this sequencer, regardless of succeeding or not",
      qualification = MetricQualification.Traffic,
    )
  )

  override def close(): Unit = state.close()
}
