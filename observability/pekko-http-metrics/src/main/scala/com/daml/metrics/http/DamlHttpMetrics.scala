// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{MetricName, MetricsContext}

class DamlHttpMetrics(metricsFactory: LabeledMetricsFactory, component: String)
    extends HttpMetrics {

  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map(Labels.ServiceLabel -> component)
  )

  override val requestsTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "requests", "Total number of HTTP requests received.")
  override val latency: Timer =
    metricsFactory.timer(httpMetricsPrefix :+ "requests", "The duration of the HTTP requests.")
  override val requestsPayloadBytes: Histogram =
    metricsFactory.histogram(
      httpMetricsPrefix :+ "requests" :+ "payload" :+ Histogram.Bytes,
      "Distribution of the sizes of payloads received in HTTP requests.",
    )
  override val responsesPayloadBytes: Histogram =
    metricsFactory.histogram(
      httpMetricsPrefix :+ "responses" :+ "payload" :+ Histogram.Bytes,
      "Distribution of the sizes of payloads sent in HTTP responses.",
    )

}
