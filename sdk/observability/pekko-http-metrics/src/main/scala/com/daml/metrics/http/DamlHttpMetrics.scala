// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricQualification
import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricsContext}

class DamlHttpMetrics(metricsFactory: LabeledMetricsFactory, component: String)
    extends HttpMetrics {

  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map(Labels.ServiceLabel -> component)
  )

  override val requestsTotal: Meter =
    metricsFactory.meter(
      MetricInfo(
        httpMetricsPrefix :+ "requests",
        "Total number of HTTP requests received.",
        MetricQualification.Debug,
      )
    )
  override val latency: Timer =
    metricsFactory.timer(
      MetricInfo(
        httpMetricsPrefix :+ "requests",
        "The duration of the HTTP requests.",
        MetricQualification.Debug,
      )
    )
  override val requestsPayloadBytes: Histogram =
    metricsFactory.histogram(
      MetricInfo(
        httpMetricsPrefix :+ "requests" :+ "payload" :+ Histogram.Bytes,
        "Distribution of the sizes of payloads received in HTTP requests.",
        MetricQualification.Debug,
      )
    )
  override val responsesPayloadBytes: Histogram =
    metricsFactory.histogram(
      MetricInfo(
        httpMetricsPrefix :+ "responses" :+ "payload" :+ Histogram.Bytes,
        "Distribution of the sizes of payloads sent in HTTP responses.",
        MetricQualification.Debug,
      )
    )

}
