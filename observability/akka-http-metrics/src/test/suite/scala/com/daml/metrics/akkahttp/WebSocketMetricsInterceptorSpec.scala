// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.metrics.akkahttp.AkkaUtils._
import com.daml.metrics.api.MetricHandle.{Histogram, Meter}
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.daml.metrics.http.WebSocketMetrics
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class WebSocketMetricsInterceptorSpec
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers {

  import WebSocketMetricsSpec._

  // test data
  private val strictTextMessageData = "test01"
  private val strictTextMessage = TextMessage.Strict(strictTextMessageData)
  private val strictTextMessageSize = strictTextMessageData.getBytes().length.toLong

  private val streamedTextMessageData = List("test", "02", "test")
  private val streamedTextMessage = TextMessage.Streamed(Source(streamedTextMessageData))
  private val streamedTextMessageSize =
    streamedTextMessageData.foldLeft(0L)((acc, s) => acc + s.getBytes.length)

  private val strictBinaryMessageData = ByteString(Array[Byte](3, 2, 1, 0, -1, -2, -3))
  private val strictBinaryMessage = BinaryMessage.Strict(strictBinaryMessageData)
  private val strictBinaryMessageSize = strictBinaryMessageData.length.toLong

  private val streamedBinaryMessageData =
    List(ByteString(Array[Byte](24, -3, 56)), ByteString(Array[Byte](127, -128)))
  private val streamedBinaryMessage = BinaryMessage.Streamed(
    Source(streamedBinaryMessageData)
  )
  private val streamedBinaryMessageSize =
    streamedBinaryMessageData.foldLeft(0L)((acc, bs) => acc + bs.length)

  private val labels = List(("label1", "abc"), ("other", "label2"))
  private val labelFilters = labels.map { case (key, value) => LabelFilter(key, value) }

  // Test flow.
  // Creates 2 deep copies of each passing message
  private val testFlow = Flow[Message].mapAsync(1)(duplicateMessage).mapConcat(identity)

  // provides an enviroment to perform the tests
  private def withDuplicatingFlowAndMetrics[T](
      f: (Flow[Message, Message, _], TestMetrics) => T
  ): T = {
    val metrics = TestMetrics()
    val duplicatingFlowWithMetrics = WebSocketMetricsInterceptor.withRateSizeMetrics(
      metrics,
      testFlow,
    )(MetricsContext(labels: _*))
    f(duplicatingFlowWithMetrics, metrics)
  }

  "websocket_message_received_total" should {
    "count passing strict text messages" in {
      checkCountThroughFlow(strictTextMessage, getMessagesReceivedTotalValue, 1L)
    }

    "count passing streamed text messages" in {
      checkCountThroughFlow(streamedTextMessage, getMessagesReceivedTotalValue, 1L)
    }

    "count passing strict binary messages" in {
      checkCountThroughFlow(strictBinaryMessage, getMessagesReceivedTotalValue, 1L)
    }

    "count passing streamed binary messages" in {
      checkCountThroughFlow(streamedBinaryMessage, getMessagesReceivedTotalValue, 1L)
    }

    "count a set of passing messages" in {
      checkCountThroughFlow(
        List[Message](
          strictTextMessage,
          streamedTextMessage,
          strictBinaryMessage,
          streamedBinaryMessage,
        ),
        getMessagesReceivedTotalValue,
        4L,
      )
    }

    "contains the configured labels" in {
      checkCountThroughFlowForLabels(
        List(strictTextMessage),
        getMessagesReceivedTotalValue,
        1L,
        labelFilters: _*
      )
    }
  }

  "websocket_message_sent_total" should {
    // The test flow duplicate the messages, so the number of sent messages is twice the number
    // of messages given as test data.

    "count passing strict text messages" in {
      checkCountThroughFlow(strictTextMessage, getMessagesSentTotalValue, 2L)
    }

    "count passing streamed text messages" in {
      checkCountThroughFlow(streamedTextMessage, getMessagesSentTotalValue, 2L)
    }

    "count passing strict binary messages" in {
      checkCountThroughFlow(strictBinaryMessage, getMessagesSentTotalValue, 2L)
    }

    "count passing streamed binary messages" in {
      checkCountThroughFlow(streamedBinaryMessage, getMessagesSentTotalValue, 2L)
    }

    "count a set of passing messages" in {
      checkCountThroughFlow(
        List[Message](
          strictTextMessage,
          streamedTextMessage,
          strictBinaryMessage,
          streamedBinaryMessage,
        ),
        getMessagesSentTotalValue,
        8L,
      )
    }

    "contains the configured labels" in {
      checkCountThroughFlowForLabels(
        List(strictTextMessage),
        getMessagesSentTotalValue,
        2L,
        labelFilters: _*
      )
    }
  }

  "websocket_message_received_bytes" should {
    "count size of passing strict text messages" in {
      checkCountThroughFlow(
        strictTextMessage,
        getMessagesReceivedBytesValues,
        Seq(strictTextMessageSize),
      )
    }

    "count size of passing streamed text messages" in {
      checkCountThroughFlow(
        streamedTextMessage,
        getMessagesReceivedBytesValues,
        Seq(streamedTextMessageSize),
      )
    }

    "count size of passing strict binary messages" in {
      checkCountThroughFlow(
        strictBinaryMessage,
        getMessagesReceivedBytesValues,
        Seq(strictBinaryMessageSize),
      )
    }

    "count size of passing streamed binary messages" in {
      checkCountThroughFlow(
        streamedBinaryMessage,
        getMessagesReceivedBytesValues,
        Seq(streamedBinaryMessageSize),
      )
    }

    "count the size of a set of passing messages" in {
      checkCountThroughFlow(
        List[Message](
          strictTextMessage,
          streamedTextMessage,
          strictBinaryMessage,
          streamedBinaryMessage,
        ),
        getMessagesReceivedBytesValues,
        Seq(
          strictTextMessageSize,
          streamedTextMessageSize,
          strictBinaryMessageSize,
          streamedBinaryMessageSize,
        ),
      )
    }

    "contains the configured labels" in {
      checkCountThroughFlowForLabels(
        List(strictTextMessage),
        getMessagesReceivedBytesValues,
        Seq(strictTextMessageSize),
        labelFilters: _*
      )
    }
  }

  "websocket_message_sent_bytes_total" should {
    // The test flow duplicate the messages, so the size of sent messages is twice the size
    // of the messages given as test data.

    "count size of passing strict text messages" in {
      checkCountThroughFlow(
        strictTextMessage,
        getMessagesSentBytesValues,
        Seq(strictTextMessageSize, strictTextMessageSize),
      )
    }

    "count size of passing streamed text messages" in {
      checkCountThroughFlow(
        streamedTextMessage,
        getMessagesSentBytesValues,
        Seq(streamedTextMessageSize, streamedTextMessageSize),
      )
    }

    "count size of passing strict binary messages" in {
      checkCountThroughFlow(
        strictBinaryMessage,
        getMessagesSentBytesValues,
        Seq(strictBinaryMessageSize, strictBinaryMessageSize),
      )
    }

    "count size of passing streamed binary messages" in {
      checkCountThroughFlow(
        streamedBinaryMessage,
        getMessagesSentBytesValues,
        Seq(streamedBinaryMessageSize, streamedBinaryMessageSize),
      )
    }

    "count the size of a set of passing messages" in {
      checkCountThroughFlow(
        List[Message](
          strictTextMessage,
          streamedTextMessage,
          strictBinaryMessage,
          streamedBinaryMessage,
        ),
        getMessagesSentBytesValues,
        Seq(
          strictTextMessageSize,
          strictTextMessageSize,
          streamedTextMessageSize,
          streamedTextMessageSize,
          strictBinaryMessageSize,
          strictBinaryMessageSize,
          streamedBinaryMessageSize,
          streamedBinaryMessageSize,
        ),
      )
    }

    "contains the configured labels" in {
      checkCountThroughFlowForLabels(
        List(strictTextMessage),
        getMessagesSentBytesValues,
        Seq(strictTextMessageSize, strictTextMessageSize),
        labelFilters: _*
      )
    }
  }

  "websocket metrics" should {
    "not impact passing strict text message" in {
      checkMessageContentThroughFlow(strictTextMessage)
    }

    "not impact passing streamed text message" in {
      checkMessageContentThroughFlow(streamedTextMessage)
    }

    "not impact passing strict binary message" in {
      checkMessageContentThroughFlow(strictBinaryMessage)
    }

    "not impact passing streamed binary message" in {
      checkMessageContentThroughFlow(streamedBinaryMessage)
    }
  }

  private def getMessagesReceivedTotalValue(metrics: TestMetrics, labels: Seq[LabelFilter]): Long =
    metrics.messagesReceivedTotalValue(labels: _*)

  private def getMessagesSentTotalValue(metrics: TestMetrics, labels: Seq[LabelFilter]): Long =
    metrics.messagesSentTotalValue(labels: _*)

  private def getMessagesReceivedBytesValues(
      metrics: TestMetrics,
      labels: Seq[LabelFilter],
  ): Seq[Long] =
    metrics.messagesReceivedBytesValues(labels: _*)

  private def getMessagesSentBytesValues(
      metrics: TestMetrics,
      labels: Seq[LabelFilter],
  ): Seq[Long] =
    metrics.messagesSentBytesValues(labels: _*)

  private def checkCountThroughFlowForLabels[T](
      messages: List[Message],
      metricExtractor: (TestMetrics, Seq[LabelFilter]) => T,
      expected: T,
      labels: LabelFilter*
  ): Future[Assertion] = {
    withDuplicatingFlowAndMetrics { (flow, metrics) =>
      val done = Source(messages).via(flow).runWith(drainStreamedMessages)
      done.map { _ =>
        metricExtractor(metrics, labels) should be(expected)
      }
    }
  }

  private def checkCountThroughFlow[T](
      messages: List[Message],
      metricExtractor: (TestMetrics, Seq[LabelFilter]) => T,
      expected: T,
  ): Future[Assertion] = {
    checkCountThroughFlowForLabels(messages, metricExtractor, expected)
  }

  private def checkCountThroughFlow[T](
      message: Message,
      metricExtractor: (TestMetrics, Seq[LabelFilter]) => T,
      expected: T,
  ): Future[Assertion] = {
    checkCountThroughFlow(List(message), metricExtractor, expected)
  }

  private def checkMessageContentThroughFlow(testMessage: Message) =
    withDuplicatingFlowAndMetrics { (flow, _) =>
      val messagesContent =
        Source.single(testMessage).via(flow).runWith(drainStreamedMessages)
      val expectedContent =
        Source(List(testMessage, testMessage)).runWith(drainStreamedMessages)
      for {
        actualMessageContent <- messagesContent
        expectedMessageContent <- expectedContent
      } yield {
        actualMessageContent should be(expectedMessageContent)
      }
    }

  // creates a deep copy of the message
  private def duplicateMessage(message: Message): Future[List[Message]] =
    message match {
      case TextMessage.Strict(text) =>
        Future(List(TextMessage.Strict(text), TextMessage.Strict(text)))
      case TextMessage.Streamed(textStream) =>
        duplicateSource(textStream, identity[String]).map { newSource =>
          List(TextMessage.Streamed(newSource), TextMessage.Streamed(newSource))
        }
      case BinaryMessage.Strict(data) =>
        Future(List(BinaryMessage.Strict(data), BinaryMessage.Strict(data)))
      case BinaryMessage.Streamed(dataStream) =>
        duplicateSource(dataStream, duplicate).map { newSource =>
          List(BinaryMessage.Streamed(newSource), BinaryMessage.Streamed(newSource))
        }
    }

  // drains a source, and return the content of each message
  private val drainStreamedMessages: Sink[Message, Future[List[List[ByteString]]]] =
    Flow[Message]
      .flatMapConcat {
        case TextMessage.Strict(text) =>
          Source.single(List(ByteString(text)))
        case TextMessage.Streamed(textStream) =>
          textStream.fold(List[ByteString]())((acc, c) => acc :+ ByteString(c))
        case BinaryMessage.Strict(data) =>
          Source.single(List(data))
        case BinaryMessage.Streamed(dataStream) =>
          dataStream.fold(List[ByteString]())((acc, c) => acc :+ c)
      }
      .toMat(Sink.fold(List[List[ByteString]]())((acc, s) => acc :+ s))((_, mat2) => mat2)

}

object WebSocketMetricsSpec extends MetricValues {

  private val metricsFactory = InMemoryMetricsFactory
  // The metrics being tested
  case class TestMetrics(
      messagesReceivedTotal: Meter,
      messagesReceivedBytes: Histogram,
      messagesSentTotal: Meter,
      messagesSentBytes: Histogram,
  ) extends WebSocketMetrics {

    def messagesReceivedTotalValue(labels: LabelFilter*): Long =
      messagesReceivedTotal.valueFilteredOnLabels(labels: _*)
    def messagesReceivedBytesValues(labels: LabelFilter*): Seq[Long] =
      messagesReceivedBytes.valuesFilteredOnLabels(labels: _*)
    def messagesSentTotalValue(labels: LabelFilter*): Long =
      messagesSentTotal.valueFilteredOnLabels(labels: _*)
    def messagesSentBytesValues(labels: LabelFilter*): Seq[Long] =
      messagesSentBytes.valuesFilteredOnLabels(labels: _*)

  }

  object TestMetrics {

    // Creates a new set of metrics, for one test
    def apply(): TestMetrics = {
      val baseName = MetricName("test")

      val receivedTotal = metricsFactory.meter(baseName :+ "received")
      val receivedBytes = metricsFactory.histogram(baseName :+ "received" :+ Histogram.Bytes)
      val sentTotal = metricsFactory.meter(baseName :+ "sent")
      val sentBytes = metricsFactory.histogram(baseName :+ "sent" :+ Histogram.Bytes)

      TestMetrics(receivedTotal, receivedBytes, sentTotal, sentBytes)
    }
  }

}
