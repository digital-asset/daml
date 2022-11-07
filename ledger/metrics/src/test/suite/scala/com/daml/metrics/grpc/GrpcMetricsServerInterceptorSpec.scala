// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.grpc

import com.daml.grpc.adapter.utils.implementations.HelloServiceAkkaImplementation
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, TestingServerInterceptors}
import com.daml.ledger.resources.TestResourceContext
import com.daml.metrics.api.testing.{MetricValues, TestingInMemoryMetricsFactory}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import com.daml.metrics.grpc.GrpcMetricsServerInterceptorSpec.TestingGrpcMetrics
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.daml.platform.testing.StreamConsumer
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpcMetricsServerInterceptorSpec
    extends AsyncFlatSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with TestResourceContext
    with MetricValues {

  private val labelsForSimpleRequest = MetricsContext(
    Map(
      "grpc_client_type" -> "unary",
      "grpc_service_name" -> "com.daml.platform.HelloService",
      "grpc_method_name" -> "Single",
      "grpc_server_type" -> "unary",
    )
  )

  private val labelsForSimpleRequestWithStatusCode = labelsForSimpleRequest.merge(
    MetricsContext(
      Map(
        "grpc_code" -> "OK"
      )
    )
  )

  behavior of "Grpc server metrics interceptor"

  it should "time call" in {
    val metrics = new TestingGrpcMetrics
    withService(metrics).use { helloService =>
      helloService.single(new HelloRequest()).map { _ =>
        metrics.callTimer.getCounts should contain theSameElementsAs Map(
          labelsForSimpleRequestWithStatusCode -> 1
        )
      }
    }
  }

  it should "mark call start and finish" in {
    val metrics = new TestingGrpcMetrics
    withService(metrics).use { helloService =>
      helloService.single(new HelloRequest()).map { _ =>
        metrics.callsStarted.valueWithContext should contain theSameElementsAs Map(
          labelsForSimpleRequest -> 1
        )
        metrics.callsFinished.valueWithContext should contain theSameElementsAs Map(
          labelsForSimpleRequestWithStatusCode -> 1
        )
      }
    }
  }

  it should "mark streaming messages" in {
    val metrics = new TestingGrpcMetrics
    withService(metrics).use { helloService =>
      val streamConsumer = new StreamConsumer[HelloResponse](observer =>
        helloService.serverStreaming(HelloRequest(3), observer)
      )
      streamConsumer.all().map { _ =>
        def meterHasValueForStreaming(
            meter: MetricHandle.Meter,
            metricsContext: MetricsContext = withStreamingLabels(labelsForSimpleRequest),
            value: Long = 1,
        ) = {
          meter.valueWithContext should contain theSameElementsAs Map(
            metricsContext -> value
          )
        }
        meterHasValueForStreaming(metrics.callsStarted)
        meterHasValueForStreaming(
          metrics.callsFinished,
          withStreamingLabels(labelsForSimpleRequestWithStatusCode),
        )
        meterHasValueForStreaming(metrics.messagesSent, value = 3)
        meterHasValueForStreaming(metrics.messagesReceived)
        meterHasValueForStreaming(metrics.messagesRequested, value = 2)
      }
    }
  }

  it should "monitor sizes of messages" in {
    val metrics = new TestingGrpcMetrics
    withService(metrics).use { helloService =>
      val payload = ByteString.copyFromUtf8("test message")
      helloService.single(new HelloRequest(payload = payload)).map { _ =>
        metrics.messagesSentSize.valuesWithContext should contain theSameElementsAs Map(
          labelsForSimpleRequest -> Seq(14)
        )
        metrics.messagesReceivedSize.valuesWithContext should contain theSameElementsAs Map(
          labelsForSimpleRequest -> Seq(14)
        )
      }
    }
  }

  private def withStreamingLabels(context: MetricsContext) = context.merge(
    MetricsContext(
      Map("grpc_server_type" -> "streaming", "grpc_method_name" -> "ServerStreaming")
    )
  )

  private def withService(metrics: TestingGrpcMetrics) = {
    TestingServerInterceptors
      .channelOwner(
        new GrpcMetricsServerInterceptor(metrics),
        new HelloServiceAkkaImplementation,
      )
      .map(HelloServiceGrpc.stub)
  }

}

object GrpcMetricsServerInterceptorSpec {

  private val metricName: MetricName = MetricName("test")

  class TestingGrpcMetrics extends GrpcServerMetrics {
    override val callTimer: MetricHandle.Timer = TestingInMemoryMetricsFactory.timer(metricName)
    override val messagesSent: MetricHandle.Meter = TestingInMemoryMetricsFactory.meter(metricName)
    override val messagesSentSize: MetricHandle.Histogram =
      TestingInMemoryMetricsFactory.histogram(metricName)
    override val messagesReceived: MetricHandle.Meter =
      TestingInMemoryMetricsFactory.meter(metricName)
    override val messagesReceivedSize: MetricHandle.Histogram =
      TestingInMemoryMetricsFactory.histogram(metricName)
    override val callsStarted: MetricHandle.Meter = TestingInMemoryMetricsFactory.meter(metricName)
    override val callsFinished: MetricHandle.Meter = TestingInMemoryMetricsFactory.meter(metricName)
    override val messagesRequested: MetricHandle.Meter =
      TestingInMemoryMetricsFactory.meter(metricName)
  }
}
