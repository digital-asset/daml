package com.daml.metrics.grpc

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}

class DamlGrpcServerMetrics(metricsFactory: Factory, component: String) extends GrpcServerMetrics {

  private val grpcServerMetricsPrefix = MetricName.Daml :+ "grpc" :+ "server"
  private implicit val metricsContext: MetricsContext = MetricsContext(
    Map("service" -> component)
  )

  override val callTimer: MetricHandle.Timer = metricsFactory.timer(grpcServerMetricsPrefix)
  override val messagesSent: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "messages" :+ "sent"
  )
  override val messagesReceived: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "messages" :+ "received"
  )
  override val messagesSentSize: MetricHandle.Histogram = metricsFactory.histogram(
    grpcServerMetricsPrefix :+ "messages" :+ "sent" :+ "bytes"
  )
  override val messagesReceivedSize: MetricHandle.Histogram = metricsFactory.histogram(
    grpcServerMetricsPrefix :+ "messages" :+ "received" :+ "bytes"
  )
  override val callsStarted: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "started"
  )
  override val callsHandled: MetricHandle.Meter = metricsFactory.meter(
    grpcServerMetricsPrefix :+ "handled"
  )

}
