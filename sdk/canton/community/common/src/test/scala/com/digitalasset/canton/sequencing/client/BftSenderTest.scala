// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.sequencing.BftSender
import com.digitalasset.canton.sequencing.BftSender.FailedToReachThreshold
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.slf4j.event.Level

import scala.concurrent.duration.*

class BftSenderTest extends FixtureAnyWordSpec with BaseTest with HasExecutionContext {

  class Env {
    val promise1 = new PromiseUnlessShutdown[Either[String, Int]]("p1", futureSupervisor)
    val promise2 = new PromiseUnlessShutdown[Either[String, Int]]("p2", futureSupervisor)
    val promise3 = new PromiseUnlessShutdown[Either[String, Int]]("p3", futureSupervisor)
    val transports: Map[String, MockTransport] = Map(
      "sequencer1" -> new MockTransport(EitherT(promise1.futureUS)),
      "sequencer2" -> new MockTransport(EitherT(promise2.futureUS)),
      "sequencer3" -> new MockTransport(EitherT(promise3.futureUS)),
    )
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgTest): Outcome =
    withFixture(test.toNoArgTest(new Env()))

  final class MockTransport(result: EitherT[FutureUnlessShutdown, String, Int]) {
    def performRequest: EitherT[FutureUnlessShutdown, String, Int] = result
  }

  private def checkNotCompleted[E, A](result: EitherT[FutureUnlessShutdown, E, A]) = {
    always(1.second) {
      result.value.isCompleted shouldBe false
    }
  }

  private def mkRequest(threshold: PositiveInt)(implicit env: Env) = {
    import env.*
    BftSender.makeRequest[String, String, MockTransport, Int, Int](
      "test",
      futureSupervisor,
      logger,
      transports,
      threshold,
      _.performRequest,
      identity,
    )
  }

  "BftSender" should {
    "gather requests from threshold-many transports" in { implicit env =>
      import env.*

      val threshold = PositiveInt.tryCreate(2)
      val result = mkRequest(threshold)

      checkNotCompleted(result)
      promise1.outcome(Right(1))
      checkNotCompleted(result)
      promise2.outcome(Right(2))
      checkNotCompleted(result)
      promise3.outcome(Right(1))

      result.valueOrFailShutdown("result").futureValue shouldBe 1
    }

    "return as soon as it has enough identical responses" in { implicit env =>
      import env.*

      val threshold = PositiveInt.tryCreate(2)
      val result = mkRequest(threshold)

      checkNotCompleted(result)
      promise1.outcome(Right(1))
      checkNotCompleted(result)
      promise2.outcome(Right(1))

      result.valueOrFailShutdown("result").futureValue shouldBe 1
    }

    "fail early if it can't get enough responses" in { env =>
      import env.*

      val threshold = PositiveInt.tryCreate(3)
      val promise4 = new PromiseUnlessShutdown[Either[String, Int]]("p4", futureSupervisor)
      val promise5 = new PromiseUnlessShutdown[Either[String, Int]]("p5", futureSupervisor)
      val transports: Map[String, MockTransport] = Map(
        "sequencer1" -> new MockTransport(EitherT(promise1.futureUS)),
        "sequencer2" -> new MockTransport(EitherT(promise2.futureUS)),
        "sequencer3" -> new MockTransport(EitherT(promise3.futureUS)),
        "sequencer4" -> new MockTransport(EitherT(promise4.futureUS)),
        "sequencer5" -> new MockTransport(EitherT(promise5.futureUS)),
      )

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.ERROR))(
        {
          val result = BftSender.makeRequest[String, String, MockTransport, Int, Int](
            "test",
            futureSupervisor,
            logger,
            transports,
            threshold,
            _.performRequest,
            identity,
          )

          val exception = new RuntimeException("BOOM")

          checkNotCompleted(result)
          promise1.outcome(Right(1))
          checkNotCompleted(result)
          promise2.outcome(Right(2))
          checkNotCompleted(result)
          promise3.outcome(Left("failed"))
          checkNotCompleted(result)
          promise4.failure(exception)

          result.value.failOnShutdown.futureValue shouldBe Left(
            FailedToReachThreshold(
              Map(1 -> Set("sequencer1"), 2 -> Set("sequencer2")),
              Map[String, Either[Throwable, String]](
                "sequencer3" -> Right("failed"),
                "sequencer4" -> Left(exception),
              ),
            )
          )
        },
        logs => {
          forExactly(1, logs) { m =>
            m.toString should include(s"test failed for sequencer4")
          }
        },
      )
    }

    "fail with shutdown if any response is a shutdown" in { implicit env =>
      import env.*

      val threshold = PositiveInt.tryCreate(3)
      val result = mkRequest(threshold)

      checkNotCompleted(result)
      promise1.outcome(Right(1))
      checkNotCompleted(result)
      promise2.shutdown()

      result.value.unwrap.futureValue shouldBe AbortedDueToShutdown
    }

    "subsequent results should not trigger errors" in { implicit env =>
      import env.*

      val threshold = PositiveInt.one
      val result = mkRequest(threshold)

      checkNotCompleted(result)
      promise1.outcome(Right(1))

      result.valueOrFailShutdown("result").futureValue shouldBe 1

      promise2.outcome(Right(1))
      promise3.outcome(Left("failed"))
    }
  }
}
