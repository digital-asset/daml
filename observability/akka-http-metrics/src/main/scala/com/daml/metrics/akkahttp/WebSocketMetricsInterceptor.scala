// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.http.scaladsl.model.ws.{Message, TextMessage, BinaryMessage}
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import com.daml.metrics.api.MetricHandle.{Histogram, Meter}
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.http.WebSocketMetrics
import com.google.common.base.Utf8

// Support to capture metrics on websockets on akka http
object WebSocketMetricsInterceptor {

  // Wraps the given flow, to capture in the given metrics, the following signals:
  //  - total number of received messages
  //  - size of the received messages
  //  - total number of sent messages
  //  - size of the sent messages
  def withRateSizeMetrics[M](
      metrics: WebSocketMetrics,
      flow: Flow[Message, Message, M],
  )(implicit mc: MetricsContext): Flow[Message, Message, M] = {
    Flow[Message]
      .map(
        messageCountAndSizeReportMetric(
          _,
          metrics.messagesReceivedTotal,
          metrics.messagesReceivedBytes,
        )
      )
      .viaMat(flow)((_, mat2) => mat2)
      .map(
        messageCountAndSizeReportMetric(
          _,
          metrics.messagesSentTotal,
          metrics.messagesSentBytes,
        )
      )
  }

  // support for message counting, and computation and report of the size of a message
  // for streaming content, creates a copy of the message, with embedded support
  private def messageCountAndSizeReportMetric(
      message: Message,
      totalMetric: Meter,
      bytesMetric: Histogram,
  )(implicit mc: MetricsContext): Message = {
    totalMetric.mark()
    message match {
      case m: BinaryMessage.Strict =>
        bytesMetric.update(m.data.length.toLong)
        m
      case m: BinaryMessage.Streamed =>
        val newStream = m.dataStream.alsoTo(
          Flow[ByteString]
            .fold(0L)((acc, d) => acc + d.length)
            .to(Sink.foreach(bytesMetric.update(_)))
        )
        BinaryMessage.Streamed(newStream)
      case m: TextMessage.Strict =>
        bytesMetric.update(Utf8.encodedLength(m.text).toLong)
        m
      case m: TextMessage.Streamed =>
        val newStream = m.textStream.alsoTo(
          Flow[String]
            .fold(0L)((acc, t) => acc + Utf8.encodedLength(t))
            .to(Sink.foreach(bytesMetric.update(_)))
        )
        TextMessage.Streamed(newStream)
    }
  }

}
