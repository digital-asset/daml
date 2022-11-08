package com.daml.metrics.grpc

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.{MetricHandle, MetricName}

class DamlGrpcServerMetrics(metricsFactory: Factory) extends GrpcServerMetrics {

  private val grpcServerMetricsPrefix = MetricName.Daml :+ "grpc" :+ "server"

  override val callTimer: MetricHandle.Timer = metricsFactory.timer(grpcServerMetricsPrefix)
  override val messagesSent: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "messages" :+ "sent"
  )
  override val messagesReceived: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "messages" :+ "received"
  )
  override val messagesRequested: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "messages" :+ "requested"
  )
  override val messagesSentSize: MetricHandle.Histogram = metricsFactory.histogram(
    grpcServerMetricsPrefix :+ "messages" :+ "sent" :+ "bytes"
  )
  override val messagesReceivedSize: MetricHandle.Histogram = metricsFactory.histogram(
    grpcServerMetricsPrefix :+ "messages" :+ "received" :+ "bytes"
  )
  override val callsStarted: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "calls" :+ "started"
  )
  override val callsFinished: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "calls" :+ "finished"
  )

}
