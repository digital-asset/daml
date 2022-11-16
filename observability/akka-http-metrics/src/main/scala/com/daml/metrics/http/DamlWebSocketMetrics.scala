// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricHandle.{Factory, Meter}
import com.daml.metrics.api.{MetricName, MetricsContext}

class DamlWebSocketMetrics(metricsFactory: Factory, component: String) extends WebSocketMetrics {

  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map("service" -> component)
  )

  override val messagesReceivedTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "websocket" :+ "messages" :+ "received")
  override val messagesReceivedBytesTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "websocket" :+ "messages" :+ "received" :+ "bytes")
  override val messagesSentTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "websocket" :+ "messages" :+ "sent")
  override val messagesSentBytesTotal: Meter =
    metricsFactory.meter(httpMetricsPrefix :+ "websocket" :+ "messages" :+ "sent" :+ "bytes")

}
