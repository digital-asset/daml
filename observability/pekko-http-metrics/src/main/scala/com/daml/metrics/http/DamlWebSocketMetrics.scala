// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Meter}
import com.daml.metrics.api.{MetricName, MetricsContext}

class DamlWebSocketMetrics(metricsFactory: LabeledMetricsFactory, component: String)
    extends WebSocketMetrics {

  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map(Labels.ServiceLabel -> component)
  )

  override val messagesReceivedTotal: Meter =
    metricsFactory.meter(
      httpMetricsPrefix :+ "websocket" :+ "messages" :+ "received",
      "Total number of received WebSocket messages.",
    )
  override val messagesReceivedBytes: Histogram =
    metricsFactory.histogram(
      httpMetricsPrefix :+ "websocket" :+ "messages" :+ "received" :+ Histogram.Bytes,
      "Distribution of the size of received WebSocket messages.",
    )
  override val messagesSentTotal: Meter =
    metricsFactory.meter(
      httpMetricsPrefix :+ "websocket" :+ "messages" :+ "sent",
      "Total number of sent WebSocket messages.",
    )
  override val messagesSentBytes: Histogram =
    metricsFactory.histogram(
      httpMetricsPrefix :+ "websocket" :+ "messages" :+ "sent" :+ Histogram.Bytes,
      "Distribution of the size of sent WebSocket messages.",
    )

}
