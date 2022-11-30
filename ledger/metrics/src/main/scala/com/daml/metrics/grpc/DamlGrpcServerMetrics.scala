// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.grpc

import com.daml.metrics.api.MetricHandle.{Factory, Histogram}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}

class DamlGrpcServerMetrics(metricsFactory: Factory, component: String) extends GrpcServerMetrics {

  private val grpcServerMetricsPrefix = MetricName.Daml :+ "grpc" :+ "server"
  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map("service" -> component)
  )

  override val callTimer: MetricHandle.Timer = metricsFactory.timer(
    grpcServerMetricsPrefix,
    "Distribution of the durations of serving gRPC requests.",
  )
  override val messagesSent: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "messages" :+ "sent",
    "Total number of gRPC messages sent (on either type of connection).",
  )
  override val messagesReceived: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "messages" :+ "received",
    "Total number of gRPC messages received (on either type of connection).",
  )
  override val messagesSentSize: MetricHandle.Histogram = metricsFactory.histogram(
    grpcServerMetricsPrefix :+ "messages" :+ "sent" :+ Histogram.Bytes,
    "Distribution of payload sizes in gRPC messages sent (both unary and streaming).",
  )
  override val messagesReceivedSize: MetricHandle.Histogram = metricsFactory.histogram(
    grpcServerMetricsPrefix :+ "messages" :+ "received" :+ Histogram.Bytes,
    "Distribution of payload sizes in gRPC messages received (both unary and streaming).",
  )
  override val callsStarted: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "started",
    "Total number of started gRPC requests (on either type of connection).",
  )
  override val callsHandled: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "handled",
    "Total number of handled gRPC requests.",
  )

}
