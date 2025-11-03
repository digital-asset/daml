// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
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

  private val activeForDocs: Gauge[Int] =
    NoOpGauge(
      MetricInfo(
        prefix :+ "active",
        summary = "Currently active requests.",
        description =
          """Currently pending GRPC requests. These can be streams or unary requests.""",
        qualification = MetricQualification.Traffic,
        labelsWithDescription = Map(
          "method" -> "The method / service name invoked.",
          "service" -> "The API the method belongs to",
        ),
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
        labelsWithDescription = Map(
          "method" -> "The method / service name limited.",
          "service" -> "The API the method belongs to",
        ),
      ),
      0,
    )

  private val activeAndLimitGauges = new TrieMap[String, (Gauge[Int], Gauge[Int])]()

  def getActiveAndLimitGauge(api: String, methodName: String): (Gauge[Int], Gauge[Int]) =
    activeAndLimitGauges.getOrElseUpdate(
      api + methodName, {
        val mc =
          metricsContext.withExtraLabels("api" -> api, "method" -> methodName, "service" -> service)
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
