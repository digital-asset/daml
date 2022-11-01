// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.http.scaladsl.model.ws.{Message, TextMessage, BinaryMessage}
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import com.daml.metrics.api.MetricHandle.Counter
import com.daml.metrics.api.MetricsContext
import com.google.common.base.Utf8

// Support to capture metrics on websockets on akka http
object WebSocketMetrics {

  // Wraps the given flow, to capture in the given metrics, the following signals:
  //  - total number of received messages
  //  - size of the received messages
  //  - total number of sent messages
  //  - size of the sent messages
  def withGoldenSignalsMetrics[M](
      receivedTotal: Counter,
      receivedBytesTotal: Counter,
      sentTotal: Counter,
      sentBytesTotal: Counter,
      flow: Flow[Message, Message, M],
  )(implicit mc: MetricsContext): Flow[Message, Message, M] = {
    Flow[Message]
      .map(messageCountAndSizeReportMetric(_, receivedTotal, receivedBytesTotal))
      .viaMat(flow)((_, mat2) => mat2)
      .map(messageCountAndSizeReportMetric(_, sentTotal, sentBytesTotal))
  }

  // support for message counting, and computation and report of the size of a message
  // for streaming content, creates a copy of the message, with embedded support
  private def messageCountAndSizeReportMetric(
      message: Message,
      totalMetric: Counter,
      bytesTotalMetric: Counter,
  )(implicit mc: MetricsContext): Message = {
    totalMetric.inc()
    message match {
      case m: BinaryMessage.Strict =>
        bytesTotalMetric.inc(m.data.length.toLong)
        m
      case m: BinaryMessage.Streamed =>
        val newStream = m.dataStream.alsoTo(
          Flow[ByteString]
            .fold(0L)((acc, d) => acc + d.length)
            .to(Sink.foreach(bytesTotalMetric.inc(_)))
        )
        BinaryMessage.Streamed(newStream)
      case m: TextMessage.Strict =>
        bytesTotalMetric.inc(Utf8.encodedLength(m.text).toLong)
        m
      case m: TextMessage.Streamed =>
        val newStream = m.textStream.alsoTo(
          Flow[String]
            .fold(0L)((acc, t) => acc + Utf8.encodedLength(t))
            .to(Sink.foreach(bytesTotalMetric.inc(_)))
        )
        TextMessage.Streamed(newStream)
    }
  }

}
