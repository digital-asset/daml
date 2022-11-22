// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package test

import akka.stream.scaladsl.{Flow, Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.lf.engine.trigger.Runner.TriggerContext
import com.daml.logging.LoggingContextOf
import com.daml.scalatest.AsyncForAll
import com.daml.util.Ctx
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.std.boolean._

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import Duration.Zero
import scala.concurrent.Future

class RunnerSpec extends AsyncWordSpec with Matchers with AsyncForAll with AkkaBeforeAndAfterAll {
  import Runner.retrying

  implicit val loggingContext: LoggingContextOf[Trigger] =
    LoggingContextOf.newLoggingContext(LoggingContextOf.label)(identity)

  "retrying" should {
    import Future.{successful => okf}

    val trialCount = 5

    def runItThrough[A, B](xs: Seq[A])(
        flow: Flow[TriggerContext[A], TriggerContext[B], _]
    ): Future[Seq[B]] =
      Source(xs).map(Ctx(loggingContext, _)).via(flow).runWith(Sink.seq).map(_.map(_.value))

    "terminate immediately on empty input" in {
      Source
        .empty[TriggerContext[Unit]]
        .via(retrying(5, _ => Zero, 8, a => okf(Some(a.value)), a => okf(a.value)))
        .runWith(Sink.seq)
        .map(_ shouldBe empty)
    }

    "ignore retryable function if no retries" in forAllAsync(trialCount) { xs: Seq[Int] =>
      runItThrough(xs)(retrying(1, _ => Zero, 1, _ => fail("retried"), a => okf(a.value + 42)))
        .map(_ should ===(xs map (_ + 42)))
    }

    "not retry if retryable succeeded" in forAllAsync(trialCount) { xs: Seq[Int] =>
      runItThrough(xs)(
        retrying(2, _ => Zero, 1, a => okf(Some(a.value + 42)), a => okf(a.value - 42))
      )
        .map(_ should ===(xs map (_ + 42)))
    }

    "if failed" should {
      "retry when parallelism is 1" in forAllAsync(trialCount) { xs: Seq[Int] =>
        runItThrough(xs)(
          retrying(
            3,
            _ => 10.milliseconds,
            1,
            a => okf((a.value % 2 == 0) option (a.value - 42)),
            a => okf(a.value + 42),
          )
        )
          .map(_ should contain theSameElementsAs xs.map(n => n + (if (n % 2 == 0) -42 else 42)))
      }

      "retry when parallelism is 6" in forAllAsync(trialCount) { xs: Seq[Int] =>
        runItThrough(xs)(
          retrying(
            3,
            _ => 10.milliseconds,
            6,
            a => okf((a.value % 2 == 0) option (a.value - 42)),
            a => okf(a.value + 42),
          )
        )
          .map(_ should contain theSameElementsAs xs.map(n => n + (if (n % 2 == 0) -42 else 42)))
      }
    }
  }
}
