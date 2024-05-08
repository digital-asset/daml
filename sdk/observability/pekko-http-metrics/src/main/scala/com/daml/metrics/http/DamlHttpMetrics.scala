// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricQualification
import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricsContext}
import com.daml.metrics.api.HistogramInventory
import com.daml.metrics.api.HistogramInventory.Item

class DamlHttpHistograms(implicit
    inventory: HistogramInventory
) {
  private[http] val httpMetricsPrefix = MetricName.Daml :+ "http"

  val latency: Item =
    Item(
      httpMetricsPrefix :+ "requests",
      "The duration of the HTTP requests.",
      MetricQualification.Debug,
    )
  val requestsPayloadBytes: Item =
    Item(
      httpMetricsPrefix :+ "requests" :+ "payload" :+ Histogram.Bytes,
      "Distribution of the sizes of payloads received in HTTP requests.",
      MetricQualification.Debug,
    )
  val responsesPayloadBytes: Item =
    Item(
      httpMetricsPrefix :+ "responses" :+ "payload" :+ Histogram.Bytes,
      "Distribution of the sizes of payloads sent in HTTP responses.",
      MetricQualification.Debug,
    )
}

class DamlHttpMetrics(
    metricsFactory: LabeledMetricsFactory,
    component: String,
) extends HttpMetrics {

  private val histograms = new DamlHttpHistograms()(new HistogramInventory)

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map(Labels.ServiceLabel -> component)
  )

  override val requestsTotal: Meter =
    metricsFactory.meter(
      MetricInfo(
        histograms.httpMetricsPrefix :+ "requests",
        "Total number of HTTP requests received.",
        MetricQualification.Debug,
      )
    )
  override val latency: Timer = metricsFactory.timer(histograms.latency.info)
  override val requestsPayloadBytes: Histogram =
    metricsFactory.histogram(histograms.requestsPayloadBytes.info)

  override val responsesPayloadBytes: Histogram =
    metricsFactory.histogram(histograms.responsesPayloadBytes.info)

}
