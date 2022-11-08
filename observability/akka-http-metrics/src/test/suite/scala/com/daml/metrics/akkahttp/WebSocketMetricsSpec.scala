// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.metrics.akkahttp.AkkaUtils._
import com.daml.metrics.api.MetricHandle.Meter
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{MetricName, MetricsContext}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class WebSocketMetricsSpec extends AsyncWordSpec with AkkaBeforeAndAfterAll with Matchers {

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
  private val labelFilter = labels.map { case (key, value) => LabelKeyValue(key, value) }

  // Test flow.
  // Creates 2 deep copies of each passing message
  private val testFlow = Flow[Message].mapAsync(1)(duplicateMessage).mapConcat(identity)

  // provides an enviroment to perform the tests
  private def withDuplicatingFlowAndMetrics[T](
      f: (Flow[Message, Message, _], TestMetrics) => T
  ): T = {
    val metrics = TestMetrics()
    val duplicatingFlowWithMetrics = WebSocketMetrics.withRateSizeMetrics(
      metrics.messagesReceivedTotal,
      metrics.messagesReceivedBytesTotal,
      metrics.messagesSentTotal,
      metrics.messagesSentBytesTotal,
      testFlow,
    )(MetricsContext(labels.toMap))
    f(duplicatingFlowWithMetrics, metrics)
  }

  "websocket_message_received_total" should {
    "count passing strict text messages" in {
      checkCountThroughFlow(strictTextMessage, getMessagesReceivedTotalValue, 1)
    }

    "count passing streamed text messages" in {
      checkCountThroughFlow(streamedTextMessage, getMessagesReceivedTotalValue, 1)
    }

    "count passing strict binary messages" in {
      checkCountThroughFlow(strictBinaryMessage, getMessagesReceivedTotalValue, 1)
    }

    "count passing streamed binary messages" in {
      checkCountThroughFlow(streamedBinaryMessage, getMessagesReceivedTotalValue, 1)
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
        4,
      )
    }

    "contains the configured labels" in {
      checkCountThroughFlowForLabels(
        List(strictTextMessage),
        getMessagesReceivedTotalValue,
        1,
        labelFilter: _*
      )
    }
  }

  "websocket_message_sent_total" should {
    // The test flow duplicate the messages, so the number of sent messages is twice the number
    // of messages given as test data.

    "count passing strict text messages" in {
      checkCountThroughFlow(strictTextMessage, getMessagesSentTotalValue, 2)
    }

    "count passing streamed text messages" in {
      checkCountThroughFlow(streamedTextMessage, getMessagesSentTotalValue, 2)
    }

    "count passing strict binary messages" in {
      checkCountThroughFlow(strictBinaryMessage, getMessagesSentTotalValue, 2)
    }

    "count passing streamed binary messages" in {
      checkCountThroughFlow(streamedBinaryMessage, getMessagesSentTotalValue, 2)
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
        8,
      )
    }

    "contains the configured labels" in {
      checkCountThroughFlowForLabels(
        List(strictTextMessage),
        getMessagesSentTotalValue,
        2,
        labelFilter: _*
      )
    }
  }

  "websocket_message_received_bytes_total" should {
    "count size of passing strict text messages" in {
      checkCountThroughFlow(
        strictTextMessage,
        getMessagesReceivedBytesTotalValue,
        strictTextMessageSize,
      )
    }

    "count size of passing streamed text messages" in {
      checkCountThroughFlow(
        streamedTextMessage,
        getMessagesReceivedBytesTotalValue,
        streamedTextMessageSize,
      )
    }

    "count size of passing strict binary messages" in {
      checkCountThroughFlow(
        strictBinaryMessage,
        getMessagesReceivedBytesTotalValue,
        strictBinaryMessageSize,
      )
    }

    "count size of passing streamed binary messages" in {
      checkCountThroughFlow(
        streamedBinaryMessage,
        getMessagesReceivedBytesTotalValue,
        streamedBinaryMessageSize,
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
        getMessagesReceivedBytesTotalValue,
        strictTextMessageSize + streamedTextMessageSize + strictBinaryMessageSize + streamedBinaryMessageSize,
      )
    }

    "contains the configured labels" in {
      checkCountThroughFlowForLabels(
        List(strictTextMessage),
        getMessagesReceivedBytesTotalValue,
        strictTextMessageSize,
        labelFilter: _*
      )
    }
  }

  "websocket_message_sent_bytes_total" should {
    // The test flow duplicate the messages, so the size of sent messages is twice the size
    // of the messages given as test data.

    "count size of passing strict text messages" in {
      checkCountThroughFlow(
        strictTextMessage,
        getMessagesSentBytesTotalValue,
        strictTextMessageSize * 2,
      )
    }

    "count size of passing streamed text messages" in {
      checkCountThroughFlow(
        streamedTextMessage,
        getMessagesSentBytesTotalValue,
        streamedTextMessageSize * 2,
      )
    }

    "count size of passing strict binary messages" in {
      checkCountThroughFlow(
        strictBinaryMessage,
        getMessagesSentBytesTotalValue,
        strictBinaryMessageSize * 2,
      )
    }

    "count size of passing streamed binary messages" in {
      checkCountThroughFlow(
        streamedBinaryMessage,
        getMessagesSentBytesTotalValue,
        streamedBinaryMessageSize * 2,
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
        getMessagesSentBytesTotalValue,
        (strictTextMessageSize + streamedTextMessageSize + strictBinaryMessageSize + streamedBinaryMessageSize) * 2,
      )
    }

    "contains the configured labels" in {
      checkCountThroughFlowForLabels(
        List(strictTextMessage),
        getMessagesSentBytesTotalValue,
        strictTextMessageSize * 2,
        labelFilter: _*
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

  // TODO def
  private val getMessagesReceivedTotalValue: (TestMetrics, Seq[LabelKeyValue]) => Long =
    (metrics: TestMetrics, labels: Seq[LabelKeyValue]) =>
      metrics.messagesReceivedTotalValue(labels: _*)

  // TODO def
  private val getMessagesSentTotalValue: (TestMetrics, Seq[LabelKeyValue]) => Long =
    (metrics: TestMetrics, labels: Seq[LabelKeyValue]) => metrics.messagesSentTotalValue(labels: _*)

  // TODO def
  private val getMessagesReceivedBytesTotalValue: (TestMetrics, Seq[LabelKeyValue]) => Long =
    (metrics: TestMetrics, labels: Seq[LabelKeyValue]) =>
      metrics.messagesReceivedBytesTotalValue(labels: _*)

  // TODO def
  private val getMessagesSentBytesTotalValue: (TestMetrics, Seq[LabelKeyValue]) => Long =
    (metrics: TestMetrics, labels: Seq[LabelKeyValue]) =>
      metrics.messagesSentBytesTotalValue(labels: _*)

  private def checkCountThroughFlowForLabels(
      messages: List[Message],
      metricExtractor: (TestMetrics, Seq[LabelKeyValue]) => Long,
      expected: Long,
      labels: LabelKeyValue*
  ): Future[Assertion] = {
    withDuplicatingFlowAndMetrics { (flow, metrics) =>
      val done = Source(messages).via(flow).runWith(drainStreamedMessages)
      done.map { _ =>
        metricExtractor(metrics, labels) should be(expected)
      }
    }
  }

  private def checkCountThroughFlow(
      messages: List[Message],
      metricExtractor: (TestMetrics, Seq[LabelKeyValue]) => Long,
      expected: Long,
  ): Future[Assertion] = {
    checkCountThroughFlowForLabels(messages, metricExtractor, expected)
  }

  private def checkCountThroughFlow(
      message: Message,
      metricExtractor: (TestMetrics, Seq[LabelKeyValue]) => Long,
      expected: Long,
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
      messagesReceivedBytesTotal: Meter,
      messagesSentTotal: Meter,
      messagesSentBytesTotal: Meter,
  ) {

    import TestMetrics._

    def messagesReceivedTotalValue(labels: LabelKeyValue*): Long =
      getCurrentValue(messagesReceivedTotal, labels: _*)
    def messagesReceivedBytesTotalValue(labels: LabelKeyValue*): Long =
      getCurrentValue(messagesReceivedBytesTotal, labels: _*)
    def messagesSentTotalValue(labels: LabelKeyValue*): Long =
      getCurrentValue(messagesSentTotal, labels: _*)
    def messagesSentBytesTotalValue(labels: LabelKeyValue*): Long =
      getCurrentValue(messagesSentBytesTotal, labels: _*)

  }

  object TestMetrics {

    // Creates a new set of metrics, for one test
    def apply(): TestMetrics = {
      val baseName = MetricName("test")

      val receivedTotal = metricsFactory.meter(baseName)
      val receivedBytesTotal = metricsFactory.meter(baseName)
      val sentTotal = metricsFactory.meter(baseName)
      val sentBytesTotal = metricsFactory.meter(baseName)

      TestMetrics(receivedTotal, receivedBytesTotal, sentTotal, sentBytesTotal)
    }
  }

}
