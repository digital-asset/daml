// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams.dispatcher

import java.lang

import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.awaitility.Awaitility.await
import org.awaitility.Duration
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScaledTimeSpans}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{FutureOutcome, Matchers, fixture}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SignalDispatcherTest
    extends fixture.AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with ScaledTimeSpans
    with AsyncTimeLimitedTests {

  "SignalDispatcher" should {

    "send a signal on subscription if requested" in { sut =>
      sut.subscribe(Some(())).runWith(Sink.head).map(_ => succeed)
    }

    "not send a signal on subscription if not requested" in { sut =>
      val s = sut.subscribe(None).runWith(TestSink.probe[Unit])
      s.request(1L)
      s.expectNoMessage(1.second)
      succeed
    }

    "output a signal when it arrives" in { sut =>
      val result = sut.subscribe(None).runWith(Sink.head).map(_ => succeed)
      sut.signal(())
      result
    }

    "output multiple signals when they arrive" in { sut =>
      val count = 10
      val result = sut.subscribe(None).take(count.toLong).runWith(Sink.seq).map(_ => succeed)
      1.to(count).foreach(_ => sut.signal(()))
      result
    }

    "remove queues from its state when the stream terminates behind them" in { sut =>
      val s = sut.subscribe(Some(())).runWith(TestSink.probe[Unit])
      s.request(1L)
      s.expectNext(())
      sut.getRunningState should have size 1L
      s.cancel()
      await("Cancellation handling")
        .atMost(Duration.TEN_SECONDS)
        .until(() => new lang.Boolean(sut.getRunningState.isEmpty))
      sut.getRunningState shouldBe empty
    }

    "remove queues from its state when closed" in { sut =>
      val s = sut.subscribe(Some(())).runWith(TestSink.probe[Unit])
      s.request(1L)
      s.expectNext(())
      sut.getRunningState should have size 1L
      sut.close()
      assertThrows[IllegalStateException](sut.getRunningState)
      assertThrows[IllegalStateException](sut.signal(()))
      s.expectComplete()
      succeed
    }
  }
  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    test.apply(SignalDispatcher[Unit]())
  override type FixtureParam = SignalDispatcher[Unit]
  override def timeLimit: Span = scaled(10.seconds)
}
