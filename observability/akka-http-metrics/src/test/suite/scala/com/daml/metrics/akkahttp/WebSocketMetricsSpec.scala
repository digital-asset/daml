// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.util.ByteString

import scala.concurrent.Future
import akka.stream.scaladsl.{Source, Flow, Sink}
import akka.http.scaladsl.model.ws.{Message, TextMessage, BinaryMessage}

import com.daml.metrics.akkahttp.AkkaUtils._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.Assertion
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll

import com.daml.metrics.api.MetricName
import com.daml.metrics.api.MetricHandle.{Counter, Histogram}

class AkkaHttpMetricsSpec extends AsyncWordSpec with AkkaBeforeAndAfterAll with Matchers {

  val strictTextMessage = TextMessage.Strict("test01")
  val streamedTextMessage = TextMessage.Streamed(Source(List("test", "02", "test")))
  val strictBinaryMessage = BinaryMessage.Strict(ByteString(Array[Byte](3, 2, 1, 0, -1, -2, -3)))
  val streamedBinaryMessage = BinaryMessage.Streamed(
    Source(List(ByteString(Array[Byte](24, -3, 56)), ByteString(Array[Byte](127, -128))))
  )

  /** The metrics being tested
    */
  case class TestMetrics(
      messagesReceivedTotal: Counter,
      messagesReceivedSizeByte: Histogram,
      messagesSentTotal: Counter,
      messagesSentSizeByte: Histogram,
  ) {

    import TestMetrics._

    def messagesReceivedTotalValue: Long = getCounterValue(messagesReceivedTotal)
    def messagesReceivedSizeByteValue: HistogramData = getHistogramValues(messagesReceivedSizeByte)
    def messagesSentTotalValue: Long = getCounterValue(messagesSentTotal)
    def messagesSentSizeByteValue: HistogramData = getHistogramValues(messagesSentSizeByte)

  }

  object TestMetrics extends TestMetricsBase {

    /** Creates a new set of metrics, for a test
      */
    def apply(): TestMetrics = {
      val testNumber = testNumbers.getAndIncrement()
      val baseName = MetricName(s"test-$testNumber")

      val receivedTotalName = baseName :+ "messages_received_total"
      val receivedSizeByteName = baseName :+ "messages_received_size_bytes"
      val sentTotalName = baseName :+ "messages_sent_total"
      val sentSizeByteName = baseName :+ "messages_sent_size_bytes"

      val receivedTotal = metricFactory.counter(receivedTotalName)
      val receivedSizeByte = metricFactory.histogram(receivedSizeByteName)
      val sentTotal = metricFactory.counter(sentTotalName)
      val sentSizeByte = metricFactory.histogram(sentSizeByteName)

      TestMetrics(receivedTotal, receivedSizeByte, sentTotal, sentSizeByte)
    }
  }

  // provides an enviroment to perform the tests
  private def withDuplicatingFlowAndMetrics[T](
      f: (Flow[Message, Message, _], TestMetrics) => T
  ): T = {
    val metrics = TestMetrics()
    val duplicatingFlowWithMetrics = WebSocketMetrics.withGoldenSignalsMetrics(
      metrics.messagesReceivedTotal,
      metrics.messagesReceivedSizeByte,
      metrics.messagesSentTotal,
      metrics.messagesSentSizeByte,
      Flow[Message].mapAsync(1)(duplicateMessage).mapConcat(identity),
    )
    f(duplicatingFlowWithMetrics, metrics)
  }

  // create a deep copy of the Message
  private def duplicateMessage(m: Message): Future[List[Message]] = {
    m match {
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
  }

  // drain a source, and return the content of each message
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
          // dataStream.map(_ => 1)
          dataStream.fold(List[ByteString]())((acc, c) => acc :+ c)
      }
      .toMat(Sink.fold(List[List[ByteString]]())((acc, s) => acc :+ s))((_, mat2) => mat2)

  private val getMessagesReceivedTotalValue: TestMetrics => Long = (metrics: TestMetrics) =>
    metrics.messagesReceivedTotalValue

  private val getMessagesSentTotalValue: TestMetrics => Long = (metrics: TestMetrics) =>
    metrics.messagesSentTotalValue

  private def checkCountThroughFlow(
      messages: List[Message],
      metricExtractor: (TestMetrics) => Long,
      expected: Long,
  ): Future[Assertion] = {
    withDuplicatingFlowAndMetrics { (flow, metrics) =>
      val done = Source(messages).via(flow).run()
      done.map { _ =>
        metricExtractor(metrics) should be(expected)
      }
    }
  }

  private def checkCountThroughFlow(
      message: Message,
      metricExtractor: TestMetrics => Long,
      expected: Long,
  ): Future[Assertion] = {
    checkCountThroughFlow(List(message), metricExtractor, expected)
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
  }

  "websocket_message_sent_total" should {
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
  }

  private val getMessagesReceivedSizeByteValue: TestMetrics => HistogramData =
    (metrics: TestMetrics) => metrics.messagesReceivedSizeByteValue

  private val getMessagesSentSizeByteValue: TestMetrics => HistogramData = (metrics: TestMetrics) =>
    metrics.messagesSentSizeByteValue

  private def checkSizeByteThroughFlow(
      messages: List[Message],
      metricExtractor: TestMetrics => HistogramData,
      expected: HistogramData,
  ): Future[Assertion] = {
    withDuplicatingFlowAndMetrics { (flow, metrics) =>
      val done = Source(messages).via(flow).runWith(drainStreamedMessages)
      done.map { _ =>
        metricExtractor(metrics) should be(expected)
      }
    }
  }

  private def checkSizeByteThroughFlow(
      message: Message,
      metricExtractor: TestMetrics => HistogramData,
      expected: HistogramData,
  ): Future[Assertion] = {
    checkSizeByteThroughFlow(List(message), metricExtractor, expected)
  }

  "websocket_message_received_size_bytes" should {
    "count size of passing strict text messages" in {
      checkSizeByteThroughFlow(
        strictTextMessage,
        getMessagesReceivedSizeByteValue,
        HistogramData(6L),
      )
    }

    "count size of passing streamed text messages" in {
      checkSizeByteThroughFlow(
        streamedTextMessage,
        getMessagesReceivedSizeByteValue,
        HistogramData(10L),
      )
    }

    "count size of passing strict binary messages" in {
      checkSizeByteThroughFlow(
        strictBinaryMessage,
        getMessagesReceivedSizeByteValue,
        HistogramData(7L),
      )
    }

    "count size of passing streamed binary messages" in {
      checkSizeByteThroughFlow(
        streamedBinaryMessage,
        getMessagesReceivedSizeByteValue,
        HistogramData(5L),
      )
    }

    "count the size of a set of passing messages" in {
      checkSizeByteThroughFlow(
        List[Message](
          strictTextMessage,
          streamedTextMessage,
          strictBinaryMessage,
          streamedBinaryMessage,
        ),
        getMessagesReceivedSizeByteValue,
        HistogramData(List(5L, 6L, 7L, 10L)),
      )
    }
  }

  "websocket_message_sent_size_bytes" should {
    "count size of passing strict text messages" in {
      checkSizeByteThroughFlow(
        strictTextMessage,
        getMessagesSentSizeByteValue,
        HistogramData(List(6L, 6L)),
      )
    }

    "count size of passing streamed text messages" in {
      checkSizeByteThroughFlow(
        streamedTextMessage,
        getMessagesSentSizeByteValue,
        HistogramData(List(10L, 10L)),
      )
    }

    "count size of passing strict binary messages" in {
      checkSizeByteThroughFlow(
        strictBinaryMessage,
        getMessagesSentSizeByteValue,
        HistogramData(List(7L, 7L)),
      )
    }

    "count size of passing streamed binary messages" in {
      checkSizeByteThroughFlow(
        streamedBinaryMessage,
        getMessagesSentSizeByteValue,
        HistogramData(List(5L, 5L)),
      )
    }

    "count the size of a set of passing messages" in {
      checkSizeByteThroughFlow(
        List[Message](
          strictTextMessage,
          streamedTextMessage,
          strictBinaryMessage,
          streamedBinaryMessage,
        ),
        getMessagesSentSizeByteValue,
        HistogramData(List(5L, 5L, 6L, 6L, 7L, 7L, 10L, 10L)),
      )
    }
  }

  private def checkMessageContentThroughFlow(m: Message) =
    withDuplicatingFlowAndMetrics { (flow, _) =>
      val messagesContent =
        Source.single(m).via(flow).runWith(drainStreamedMessages)
      val expectedContent =
        Source(List(m, m)).runWith(drainStreamedMessages)
      for {
        m <- messagesContent
        e <- expectedContent
      } yield {
        m should be(e)
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

}
