// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.noop.NoOpGauge
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.daml.metrics.grpc.GrpcServerMetrics

import scala.collection.concurrent.TrieMap

// TODO(#29098) move into Grpc Server metrics
class ActiveRequestsMetrics(
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
    service: String,
)(implicit
    metricsContext: MetricsContext
) {

  private val prefix = MetricName.Daml :+ "grpc" :+ "server" :+ "requests"
  private val labels = Map(
    "method" -> "The method / service name limited.",
    "service" -> "The API the method belongs to",
    "api" -> "The API the method belongs to",
  )

  private val activeForDocs: Gauge[Int] =
    NoOpGauge(
      MetricInfo(
        prefix :+ "active",
        summary = "Currently active requests.",
        description =
          """Currently pending GRPC requests. These can be streams or unary requests.""",
        qualification = MetricQualification.Traffic,
        labelsWithDescription = labels,
      ),
      0,
    )

  private val limitForDocs: Gauge[Int] =
    NoOpGauge(
      MetricInfo(
        prefix :+ "limit",
        summary = "Limit for concurrent requests per method.",
        description = """Limits for concurrent requests. These can be streams or unary requests.""",
        qualification = MetricQualification.Traffic,
        labelsWithDescription = labels,
      ),
      0,
    )

  val rejections: Counter = openTelemetryMetricsFactory.counter(
    MetricInfo(
      prefix :+ "rejections",
      summary = "Number of rejected requests due to active request limits.",
      description =
        "Counts the number of requests rejected because the active request limit was reached.",
      qualification = MetricQualification.Saturation,
      labelsWithDescription = labels,
    )
  )
  def mkContext(api: String, methodName: String): MetricsContext =
    metricsContext.withExtraLabels("api" -> api, "method" -> methodName, "service" -> service)

  private val activeAndLimitGauges = new TrieMap[String, (Gauge[Int], Gauge[Int])]()

  def getActiveAndLimitGauge(api: String, methodName: String): (Gauge[Int], Gauge[Int]) =
    activeAndLimitGauges.getOrElseUpdate(
      // distinguish by api to avoid conflicts around method names and counts
      api + "::" + methodName, {
        val mc = mkContext(api, methodName)
        (
          openTelemetryMetricsFactory.gauge(activeForDocs.info, 0)(mc),
          openTelemetryMetricsFactory.gauge(limitForDocs.info, 0)(mc),
        )
      },
    )

}

object ActiveRequestsMetrics {
  // TODO(#29098) merge again
  type GrpcServerMetricsX = (GrpcServerMetrics, ActiveRequestsMetrics)

}
