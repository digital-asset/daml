// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.grpc

import com.daml.metrics.api.MetricQualification
import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricName, MetricsContext}
import com.daml.metrics.grpc.DamlGrpcServerMetrics.GrpcServerMetricsPrefix

object DamlGrpcServerMetrics {
  val GrpcServerMetricsPrefix = MetricName.Daml :+ "grpc" :+ "server"
}

class DamlGrpcServerMetrics(
    metricsFactory: LabeledMetricsFactory,
    service: String,
) extends GrpcServerMetrics {

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map("service" -> service)
  )

  override val callTimer: MetricHandle.Timer = metricsFactory.timer(
    MetricInfo(
      GrpcServerMetricsPrefix,
      summary = "Distribution of the durations of serving gRPC requests.",
      qualification = MetricQualification.Traffic,
    )
  )
  override val messagesSent: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      GrpcServerMetricsPrefix :+ "messages" :+ "sent",
      summary = "Total number of gRPC messages sent (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )
  override val messagesReceived: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      GrpcServerMetricsPrefix :+ "messages" :+ "received",
      summary = "Total number of gRPC messages received (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )
  override val messagesSentSize: MetricHandle.Histogram = metricsFactory.histogram(
    MetricInfo(
      GrpcServerMetricsPrefix :+ "messages" :+ "sent" :+ Histogram.Bytes,
      summary = "Distribution of payload sizes in gRPC messages sent (both unary and streaming).",
      qualification = MetricQualification.Traffic,
    )
  )
  override val messagesReceivedSize: MetricHandle.Histogram = metricsFactory.histogram(
    MetricInfo(
      GrpcServerMetricsPrefix :+ "messages" :+ "received" :+ Histogram.Bytes,
      summary =
        "Distribution of payload sizes in gRPC messages received (both unary and streaming).",
      qualification = MetricQualification.Traffic,
    )
  )
  override val callsStarted: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      GrpcServerMetricsPrefix :+ "started",
      summary = "Total number of started gRPC requests (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )
  override val callsHandled: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      GrpcServerMetricsPrefix :+ "handled",
      summary = "Total number of handled gRPC requests.",
      qualification = MetricQualification.Traffic,
    )
  )

}
