// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.daml.metrics.http.{
  DamlHttpHistograms,
  DamlHttpMetrics,
  DamlWebSocketMetrics,
  DamlWebSocketsHistograms,
}

import scala.annotation.unused

object HttpApiMetrics {
  lazy val ForTesting =
    new HttpApiMetrics(
      new HttpApiHistograms(MetricName("test"))(new HistogramInventory),
      NoOpMetricsFactory,
    )

  final val ComponentName = "json_api"
}

class HttpApiHistograms(parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  @unused
  private val _http: DamlHttpHistograms = new DamlHttpHistograms()
  @unused
  private val _webSockets: DamlWebSocketsHistograms = new DamlWebSocketsHistograms()

  val prefix: MetricName = parent :+ "http_json_api"
}

class HttpApiMetrics(
    parent: HttpApiHistograms,
    labeledMetricsFactory: LabeledMetricsFactory,
) {
  import HttpApiMetrics.*

  val prefix: MetricName = parent.prefix

  val http = new DamlHttpMetrics(labeledMetricsFactory, ComponentName)
  val websocket = new DamlWebSocketMetrics(labeledMetricsFactory, ComponentName)

  val health = new HealthMetrics(labeledMetricsFactory)
}
