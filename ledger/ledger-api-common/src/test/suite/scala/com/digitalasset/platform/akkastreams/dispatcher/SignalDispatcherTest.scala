// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.akkastreams.dispatcher

import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.awaitility.Awaitility.await
import org.awaitility.Duration
import org.scalatest.FutureOutcome
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScaledTimeSpans}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.lang
import scala.concurrent.Await

class SignalDispatcherTest
    extends FixtureAsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with ScaledTimeSpans
    with AsyncTimeLimitedTests {

  "SignalDispatcher" should {

    "send a signal on subscription if requested" in { sut =>
      sut.subscribe(true).runWith(Sink.head).map(_ => succeed)
    }

    "not send a signal on subscription if not requested" in { sut =>
      val s = sut.subscribe(false).runWith(TestSink.probe[SignalDispatcher.Signal])
      s.request(1L)
      s.expectNoMessage(1.second)
      succeed
    }

    "output a signal when it arrives" in { sut =>
      val result = sut.subscribe(false).runWith(Sink.head).map(_ => succeed)
      sut.signal()
      result
    }

    "output multiple signals when they arrive" in { sut =>
      val count = 10
      val result = sut.subscribe(false).take(count.toLong).runWith(Sink.seq).map(_ => succeed)
      1.to(count).foreach(_ => sut.signal())
      result
    }

    "remove queues from its state when the stream terminates behind them" in { sut =>
      val s = sut.subscribe(true).runWith(TestSink.probe[SignalDispatcher.Signal])
      s.request(1L)
      s.expectNext(SignalDispatcher.Signal)
      sut.getRunningState should have size 1L
      s.cancel()
      await("Cancellation handling")
        .atMost(Duration.TEN_SECONDS)
        .until(() => lang.Boolean.valueOf(sut.getRunningState.isEmpty))
      sut.getRunningState shouldBe empty
    }

    "remove queues from its state when shutdown" in { sut =>
      val s = sut.subscribe(true).runWith(TestSink.probe[SignalDispatcher.Signal])
      s.request(1L)
      s.expectNext(SignalDispatcher.Signal)
      sut.getRunningState should have size 1L
      sut.shutdown()
      assertThrows[IllegalStateException](sut.getRunningState)
      assertThrows[IllegalStateException](sut.signal())
      s.expectComplete()
      succeed
    }

    "remove queues from its state when failed" in { sut =>
      val s = sut.subscribe(true).runWith(TestSink.probe[SignalDispatcher.Signal])
      s.request(1L)
      s.expectNext(SignalDispatcher.Signal)
      sut.getRunningState should have size 1L

      val failure = new RuntimeException("Some failure")

      // Check fail does not return a failed future
      Await.result(sut.fail(failure), 10.seconds)

      assertThrows[IllegalStateException](sut.getRunningState)
      assertThrows[IllegalStateException](sut.signal())
      s.expectError(failure)
      succeed
    }
  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    test.apply(SignalDispatcher())
  override type FixtureParam = SignalDispatcher
  override def timeLimit: Span = scaled(10.seconds)
}
