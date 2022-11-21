// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricHandle.{Factory, Histogram, Meter, Timer}
import com.daml.metrics.api.{MetricName, MetricsContext}

class DamlHttpMetrics(metricsFactory: Factory, component: String) extends HttpMetrics {

  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map(Labels.ServiceLabel -> component)
  )

  override val requestsTotal: Meter = metricsFactory.meter(httpMetricsPrefix :+ "requests")
  override val latency: Timer = metricsFactory.timer(httpMetricsPrefix :+ "requests")
  override val requestsPayloadBytes: Histogram =
    metricsFactory.histogram(httpMetricsPrefix :+ "requests" :+ "payload" :+ Histogram.Bytes)
  override val responsesPayloadBytes: Histogram =
    metricsFactory.histogram(httpMetricsPrefix :+ "responses" :+ "payload" :+ Histogram.Bytes)

}
