// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.grpc

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.{
  HistogramInventory,
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory}

class DamlGrpcServerHistograms(implicit
    inventory: HistogramInventory
) {
  private[grpc] val prefix = MetricName.Daml :+ "grpc" :+ "server"

  val damlGrpcServerCallTimer: Item = Item(
    MetricName.Daml :+ "grpc",
    MetricName("server"),
    summary = "Distribution of the durations of serving gRPC requests.",
    qualification = MetricQualification.Latency,
  )
  val damlGrpcServerReceived: Item = Item(
    prefix :+ "messages" :+ "received",
    Histogram.Bytes,
    summary = "Distribution of payload sizes in gRPC messages received (both unary and streaming).",
    qualification = MetricQualification.Traffic,
  )
  val damlGrpcServerSent: Item = Item(
    prefix :+ "messages" :+ "sent",
    Histogram.Bytes,
    summary = "Distribution of payload sizes in gRPC messages sent (both unary and streaming).",
    qualification = MetricQualification.Traffic,
  )

}

class DamlGrpcServerMetrics(
    metricsFactory: LabeledMetricsFactory,
    service: String,
) extends GrpcServerMetrics {

  private val histograms = new DamlGrpcServerHistograms()(new HistogramInventory)

  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map("service" -> service)
  )

  override val callTimer: MetricHandle.Timer =
    metricsFactory.timer(histograms.damlGrpcServerCallTimer.info)
  override val messagesSent: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      histograms.prefix :+ "messages" :+ "sent",
      summary = "Total number of gRPC messages sent (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )
  override val messagesReceived: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      histograms.prefix :+ "messages" :+ "received",
      summary = "Total number of gRPC messages received (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )
  override val messagesSentSize: MetricHandle.Histogram = metricsFactory.histogram(
    histograms.damlGrpcServerSent.info
  )
  override val messagesReceivedSize: MetricHandle.Histogram = metricsFactory.histogram(
    histograms.damlGrpcServerReceived.info
  )
  override val callsStarted: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      histograms.prefix :+ "started",
      summary = "Total number of started gRPC requests (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )
  override val callsHandled: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      histograms.prefix :+ "handled",
      summary = "Total number of handled gRPC requests.",
      qualification = MetricQualification.Traffic,
    )
  )

}
