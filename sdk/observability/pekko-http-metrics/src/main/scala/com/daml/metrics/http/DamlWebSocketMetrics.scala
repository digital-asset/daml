// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricQualification
import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Meter}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricsContext}
import com.daml.metrics.api.HistogramInventory
import com.daml.metrics.api.HistogramInventory.Item

class DamlWebSocketsHistograms(implicit
    inventory: HistogramInventory
) {
  private[http] val websocketMetricsPrefix = MetricName.Daml :+ "http" :+ "websocket" :+ "messages"

  val messagesReceivedBytes: Item =
    Item(
      websocketMetricsPrefix :+ "received" :+ Histogram.Bytes,
      "Distribution of the size of received WebSocket messages.",
      MetricQualification.Debug,
    )

  val messagesSentBytes: Item =
    Item(
      websocketMetricsPrefix :+ "sent" :+ Histogram.Bytes,
      "Distribution of the size of sent WebSocket messages.",
      MetricQualification.Debug,
    )

}

class DamlWebSocketMetrics(
    metricsFactory: LabeledMetricsFactory,
    component: String,
) extends WebSocketMetrics {

  private val histograms = new DamlWebSocketsHistograms()(new HistogramInventory)

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map(Labels.ServiceLabel -> component)
  )

  override val messagesReceivedTotal: Meter =
    metricsFactory.meter(
      MetricInfo(
        histograms.websocketMetricsPrefix :+ "received",
        "Total number of received WebSocket messages.",
        MetricQualification.Debug,
      )
    )
  override val messagesReceivedBytes: Histogram =
    metricsFactory.histogram(histograms.messagesReceivedBytes.info)

  override val messagesSentTotal: Meter =
    metricsFactory.meter(
      MetricInfo(
        histograms.websocketMetricsPrefix :+ "sent",
        "Total number of sent WebSocket messages.",
        MetricQualification.Debug,
      )
    )
  override val messagesSentBytes: Histogram =
    metricsFactory.histogram(histograms.messagesSentBytes.info)

}
