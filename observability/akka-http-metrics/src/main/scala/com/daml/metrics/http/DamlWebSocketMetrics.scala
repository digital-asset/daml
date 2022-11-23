// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricHandle.{Factory, Histogram, Meter}
import com.daml.metrics.api.{MetricName, MetricsContext}

class DamlWebSocketMetrics(metricsFactory: Factory, component: String) extends WebSocketMetrics {

  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map(Labels.ServiceLabel -> component)
  )

  override val messagesReceivedTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "websocket" :+ "messages" :+ "received")
  override val messagesReceivedBytes: Histogram =
    metricsFactory.histogram(
      httpMetricsPrefix :+ "websocket" :+ "messages" :+ "received" :+ Histogram.Bytes
    )
  override val messagesSentTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "websocket" :+ "messages" :+ "sent")
  override val messagesSentBytes: Histogram =
    metricsFactory.histogram(
      httpMetricsPrefix :+ "websocket" :+ "messages" :+ "sent" :+ Histogram.Bytes
    )

}
