// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.grpc

import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Histogram, Meter, Timer}
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricsContext.{withExtraMetricLabels, withMetricLabels}
import com.google.protobuf.{GeneratedMessage => JavaGeneratedMessage}
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ServerCall.Listener
import io.grpc.{
  ForwardingServerCallListener,
  Metadata,
  ServerCall,
  ServerCallHandler,
  ServerInterceptor,
  Status,
}
import scalapb.{GeneratedMessage => ScalapbGeneratedMessage}

class GrpcMetricsServerInterceptor(metrics: GrpcServerMetrics) extends ServerInterceptor {

  override def interceptCall[ReqT, RespT](
      serverCall: ServerCall[ReqT, RespT],
      metadata: Metadata,
      serverCallHandler: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val methodDescriptor = serverCall.getMethodDescriptor
    val clientType = if (methodDescriptor.getType.clientSendsOneMessage()) "unary" else "streaming"
    val serverType = if (methodDescriptor.getType.serverSendsOneMessage()) "unary" else "streaming"
    withMetricLabels(
      "grpc_service_name" -> methodDescriptor.getServiceName,
      "grpc_method_name" -> methodDescriptor.getBareMethodName,
      "grpc_client_type" -> clientType,
      "grpc_server_type" -> serverType,
    ) { implicit mc =>
      val timerHandle = metrics.callTimer.startAsync()
      metrics.callsStarted.mark()
      val metricsServerCall = new MetricsCall(
        serverCall,
        timerHandle,
        metrics.messagesSent,
        metrics.messagesSentSize,
        metrics.callsFinished,
        metrics.messagesRequested,
      )
      new MonitoringServerCallListener(
        serverCallHandler.startCall(metricsServerCall, metadata),
        metrics.messagesReceived,
        metrics.messagesReceivedSize,
      )
    }
  }

  private final class MonitoringServerCallListener[T](
      val delegate: Listener[T],
      messagesReceived: Meter,
      messagesReceivedSize: Histogram,
  )(implicit
      metricsContext: MetricsContext
  ) extends ForwardingServerCallListener[T] {

    override def onMessage(message: T): Unit = {
      super.onMessage(message)
      updateHistogramWithSerializedSize(messagesReceivedSize, message)
      messagesReceived.mark()
    }
  }

  private final class MetricsCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      timer: TimerHandle,
      messagesSent: Meter,
      messagesSentSize: Histogram,
      callsClosed: Meter,
      messagesRequested: Meter,
  )(implicit metricsContext: MetricsContext)
      extends SimpleForwardingServerCall[ReqT, RespT](call) {

    override def request(numMessages: Int): Unit = {
      super.request(numMessages)
      messagesRequested.mark(numMessages.toLong)
    }

    override def sendMessage(message: RespT): Unit = {
      super.sendMessage(message)
      updateHistogramWithSerializedSize(messagesSentSize, message)
      messagesSent.mark()
    }

    override def close(status: Status, trailers: Metadata): Unit = {
      super.close(status, trailers)
      withExtraMetricLabels("grpc_code" -> status.getCode.toString) { implicit metricsContext =>
        callsClosed.mark()
        timer.stop()
      }
    }
  }

  private def updateHistogramWithSerializedSize[T](
      histogram: Histogram,
      message: T,
  )(implicit metricsContext: MetricsContext): Unit = {
    message match {
      case generatedMessage: JavaGeneratedMessage =>
        histogram.update(generatedMessage.getSerializedSize)
      case generatedMessage: ScalapbGeneratedMessage =>
        histogram.update(generatedMessage.serializedSize)
      case _ =>
    }
  }
}

trait GrpcServerMetrics {
  val callTimer: Timer
  val messagesSent: Meter
  val messagesSentSize: Histogram
  val messagesReceived: Meter
  val messagesReceivedSize: Histogram
  val callsStarted: Meter
  val callsFinished: Meter
  val messagesRequested: Meter
}
