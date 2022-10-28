// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import com.google.common.base.Utf8

import akka.util.ByteString
import akka.stream.scaladsl.{Flow, Sink}
import akka.http.scaladsl.model.ws.{Message, TextMessage, BinaryMessage}

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricHandle.{Counter, Histogram}

/** Support to capture metrics on websockets on akka http
  */
object WebSocketMetrics {

  /** Wraps the given flow, to capture in the given metrics, the following signals:
    * - total number of received messages
    * - size of the received messages
    * - total number of sent messages
    * - size of the sent messages
    */
  def withGoldenSignalsMetrics[M](
      wsReceivedTotal: Counter,
      wsReceivedSizeByte: Histogram,
      wsSentTotal: Counter,
      wsSentSizeByte: Histogram,
      flow: Flow[Message, Message, M],
  )(implicit mc: MetricsContext): Flow[Message, Message, M] = {
    Flow[Message]
      .map(messageCountAndSizeReportMetric(_, wsReceivedTotal, wsReceivedSizeByte))
      .viaMat(flow)((_, mat2) => mat2)
      .map(messageCountAndSizeReportMetric(_, wsSentTotal, wsSentSizeByte))
  }

  // support for message counting, and computation and report of the size of a message
  // for streaming content, creates a copy of the message, with embedded support
  private def messageCountAndSizeReportMetric(
      message: Message,
      totalMetric: Counter,
      sizeByteMetric: Histogram,
  )(implicit mc: MetricsContext): Message = {
    totalMetric.inc()
    message match {
      case m: BinaryMessage.Strict =>
        sizeByteMetric.update(m.data.length)
        m
      case m: BinaryMessage.Streamed =>
        val newStream = m.dataStream.alsoTo(
          Flow[ByteString]
            .fold(0)((acc, d) => acc + d.length)
            .to(Sink.foreach(sizeByteMetric.update(_)))
        )
        BinaryMessage.Streamed(newStream)
      case m: TextMessage.Strict =>
        sizeByteMetric.update(Utf8.encodedLength(m.text))
        m
      case m: TextMessage.Streamed =>
        val newStream = m.textStream.alsoTo(
          Flow[String]
            .fold(0)((acc, t) => acc + Utf8.encodedLength(t))
            .to(Sink.foreach { sizeByteMetric.update(_) })
        )
        TextMessage.Streamed(newStream)
    }
  }

}
