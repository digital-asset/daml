// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.grpc

import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import com.daml.metrics.grpc.DamlGrpcServerMetrics.GrpcServerMetricsPrefix

object DamlGrpcServerMetrics {
  val GrpcServerMetricsPrefix = MetricName.Daml :+ "grpc" :+ "server"
}

class DamlGrpcServerMetrics(
    metricsFactory: LabeledMetricsFactory,
    component: String,
) extends GrpcServerMetrics {

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map("service" -> component)
  )

  override val callTimer: MetricHandle.Timer = metricsFactory.timer(
    GrpcServerMetricsPrefix,
    "Distribution of the durations of serving gRPC requests.",
  )
  override val messagesSent: MetricHandle.Meter = metricsFactory.meter(
    GrpcServerMetricsPrefix :+ "messages" :+ "sent",
    "Total number of gRPC messages sent (on either type of connection).",
  )
  override val messagesReceived: MetricHandle.Meter = metricsFactory.meter(
    GrpcServerMetricsPrefix :+ "messages" :+ "received",
    "Total number of gRPC messages received (on either type of connection).",
  )
  override val messagesSentSize: MetricHandle.Histogram = metricsFactory.histogram(
    GrpcServerMetricsPrefix :+ "messages" :+ "sent" :+ Histogram.Bytes,
    "Distribution of payload sizes in gRPC messages sent (both unary and streaming).",
  )
  override val messagesReceivedSize: MetricHandle.Histogram = metricsFactory.histogram(
    GrpcServerMetricsPrefix :+ "messages" :+ "received" :+ Histogram.Bytes,
    "Distribution of payload sizes in gRPC messages received (both unary and streaming).",
  )
  override val callsStarted: MetricHandle.Meter = metricsFactory.meter(
    GrpcServerMetricsPrefix :+ "started",
    "Total number of started gRPC requests (on either type of connection).",
  )
  override val callsHandled: MetricHandle.Meter = metricsFactory.meter(
    GrpcServerMetricsPrefix :+ "handled",
    "Total number of handled gRPC requests.",
  )

}
