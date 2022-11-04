// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.stream.scaladsl.{Source, Flow, Sink}
import akka.http.scaladsl.model.ws.{Message, TextMessage, BinaryMessage}
import akka.util.ByteString
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.metrics.api.{MetricsContext, MetricName}
import com.daml.metrics.api.MetricHandle.Counter
import com.daml.metrics.akkahttp.AkkaUtils._
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scala.concurrent.Future

class AkkaHttpMetricsSpec extends AsyncWordSpec with AkkaBeforeAndAfterAll with Matchers {

  import AkkaHttpMetricsSpec._

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
    )(MetricsContext.Empty)
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

  private val getMessagesReceivedTotalValue: TestMetrics => Long = (metrics: TestMetrics) =>
    metrics.messagesReceivedTotalValue

  private val getMessagesSentTotalValue: TestMetrics => Long = (metrics: TestMetrics) =>
    metrics.messagesSentTotalValue

  private val getMessagesReceivedBytesTotalValue: TestMetrics => Long =
    (metrics: TestMetrics) => metrics.messagesReceivedBytesTotalValue

  private val getMessagesSentBytesTotalValue: TestMetrics => Long = (metrics: TestMetrics) =>
    metrics.messagesSentBytesTotalValue

  private def checkCountThroughFlow(
      messages: List[Message],
      metricExtractor: (TestMetrics) => Long,
      expected: Long,
  ): Future[Assertion] = {
    withDuplicatingFlowAndMetrics { (flow, metrics) =>
      val done = Source(messages).via(flow).runWith(drainStreamedMessages)
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

object AkkaHttpMetricsSpec {

  // The metrics being tested
  case class TestMetrics(
      messagesReceivedTotal: Counter,
      messagesReceivedBytesTotal: Counter,
      messagesSentTotal: Counter,
      messagesSentBytesTotal: Counter,
  ) {

    import TestMetrics._

    def messagesReceivedTotalValue: Long = getCounterValue(messagesReceivedTotal)
    def messagesReceivedBytesTotalValue: Long = getCounterValue(messagesReceivedBytesTotal)
    def messagesSentTotalValue: Long = getCounterValue(messagesSentTotal)
    def messagesSentBytesTotalValue: Long = getCounterValue(messagesSentBytesTotal)

  }

  object TestMetrics extends TestMetricsBase {

    // Creates a new set of metrics, for one test
    def apply(): TestMetrics = {
      val testNumber = testNumbers.getAndIncrement()
      val baseName = MetricName(s"test-$testNumber")

      val receivedTotalName = baseName :+ "messages_received_total"
      val receivedBytesTotalName = baseName :+ "messages_received_bytes_total"
      val sentTotalName = baseName :+ "messages_sent_total"
      val sentBytesTotalName = baseName :+ "messages_sent_bytes_total"

      val receivedTotal = metricFactory.counter(receivedTotalName)
      val receivedBytesTotal = metricFactory.counter(receivedBytesTotalName)
      val sentTotal = metricFactory.counter(sentTotalName)
      val sentBytesTotal = metricFactory.counter(sentBytesTotalName)

      TestMetrics(receivedTotal, receivedBytesTotal, sentTotal, sentBytesTotal)
    }
  }

}
