// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.grpc

import com.daml.grpc.adapter.utils.implementations.HelloServicePekkoImplementation
import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.testing.utils.{PekkoBeforeAndAfterAll, TestingServerInterceptors}
import com.daml.ledger.resources.TestResourceContext
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import com.daml.metrics.grpc.GrpcMetricsServerInterceptorSpec.TestingGrpcMetrics
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.google.protobuf.ByteString
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpcMetricsServerInterceptorSpec
    extends AsyncFlatSpec
    with Matchers
    with PekkoBeforeAndAfterAll
    with TestResourceContext
    with MetricValues
    with Eventually {

  private val labelsForSimpleRequest = MetricsContext(
    Map(
      GrpcMetricsServerInterceptor.MetricsGrpcClientType -> GrpcMetricsServerInterceptor.MetricsRequestTypeUnary,
      GrpcMetricsServerInterceptor.MetricsGrpcServiceName -> "com.daml.platform.HelloService",
      GrpcMetricsServerInterceptor.MetricsGrpcMethodName -> "Single",
      GrpcMetricsServerInterceptor.MetricsGrpcServerType -> GrpcMetricsServerInterceptor.MetricsRequestTypeUnary,
    )
  )

  private val labelsForSimpleRequestWithStatusCode = labelsForSimpleRequest.merge(
    MetricsContext(
      Map(
        GrpcMetricsServerInterceptor.MetricsGrpcResponseCode -> "OK"
      )
    )
  )

  behavior of "Grpc server metrics interceptor"

  it should "time call" in {
    val metrics = new TestingGrpcMetrics
    withService(metrics).use { helloService =>
      helloService.single(new HelloRequest()).map { _ =>
        eventually {
          metrics.callTimer.countsWithContext should contain theSameElementsAs Map(
            labelsForSimpleRequestWithStatusCode -> 1
          )
        }
      }
    }
  }

  it should "mark call start and finish" in {
    val metrics = new TestingGrpcMetrics
    withService(metrics).use { helloService =>
      helloService.single(new HelloRequest()).map { _ =>
        eventually {
          metrics.callsStarted.valuesWithContext should contain theSameElementsAs Map(
            labelsForSimpleRequest -> 1
          )
          metrics.callsHandled.valuesWithContext should contain theSameElementsAs Map(
            labelsForSimpleRequestWithStatusCode -> 1
          )
        }
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
          meter.valuesWithContext should contain theSameElementsAs Map(
            metricsContext -> value
          )
        }
        eventually {
          meterHasValueForStreaming(metrics.callsStarted)
          meterHasValueForStreaming(
            metrics.callsHandled,
            withStreamingLabels(labelsForSimpleRequestWithStatusCode),
          )
          meterHasValueForStreaming(metrics.messagesSent, value = 3)
          meterHasValueForStreaming(metrics.messagesReceived)
        }
      }
    }
  }

  it should "monitor sizes of messages" in {
    val metrics = new TestingGrpcMetrics
    withService(metrics).use { helloService =>
      val payload = ByteString.copyFromUtf8("test message")
      val request = new HelloRequest(payload = payload)
      helloService.single(request).map { response =>
        eventually {
          metrics.messagesSentSize.valuesWithContext should contain theSameElementsAs Map(
            labelsForSimpleRequest -> Seq(request.serializedSize)
          )
          metrics.messagesReceivedSize.valuesWithContext should contain theSameElementsAs Map(
            labelsForSimpleRequest -> Seq(response.serializedSize)
          )
        }
      }
    }
  }

  private def withStreamingLabels(context: MetricsContext) = context.merge(
    MetricsContext(
      Map(
        GrpcMetricsServerInterceptor.MetricsGrpcServerType -> GrpcMetricsServerInterceptor.MetricsRequestTypeStreaming,
        GrpcMetricsServerInterceptor.MetricsGrpcMethodName -> "ServerStreaming",
      )
    )
  )

  private def withService(metrics: TestingGrpcMetrics) = {
    TestingServerInterceptors
      .channelOwner(
        new GrpcMetricsServerInterceptor(metrics),
        new HelloServicePekkoImplementation,
      )
      .map(HelloServiceGrpc.stub)
  }

}

object GrpcMetricsServerInterceptorSpec {

  private val metricName: MetricName = MetricName("test")

  class TestingGrpcMetrics extends GrpcServerMetrics {
    override val callTimer: MetricHandle.Timer =
      InMemoryMetricsFactory
        .timer(metricName)
    override val messagesSent: MetricHandle.Meter = InMemoryMetricsFactory.meter(metricName)
    override val messagesSentSize: MetricHandle.Histogram =
      InMemoryMetricsFactory.histogram(metricName)
    override val messagesReceived: MetricHandle.Meter =
      InMemoryMetricsFactory.meter(metricName)
    override val messagesReceivedSize: MetricHandle.Histogram =
      InMemoryMetricsFactory.histogram(metricName)
    override val callsStarted: MetricHandle.Meter = InMemoryMetricsFactory.meter(metricName)
    override val callsHandled: MetricHandle.Meter = InMemoryMetricsFactory.meter(metricName)
  }
}
