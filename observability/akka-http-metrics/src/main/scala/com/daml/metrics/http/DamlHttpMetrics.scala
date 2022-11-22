// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricHandle.{Factory, Meter, Timer}
import com.daml.metrics.api.{MetricName, MetricsContext}

class DamlHttpMetrics(metricsFactory: Factory, component: String) extends HttpMetrics {

  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map(Labels.ServiceLabel -> component)
  )

  override val requestsTotal: Meter = metricsFactory.meter(httpMetricsPrefix :+ "requests")
  override val errorsTotal: Meter = metricsFactory.meter(httpMetricsPrefix :+ "errors")
  override val latency: Timer = metricsFactory.timer(httpMetricsPrefix :+ "requests")
  override val requestsPayloadBytesTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "requests" :+ "payload" :+ "bytes")
  override val responsesPayloadBytesTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "responses" :+ "payload" :+ "bytes")

}
