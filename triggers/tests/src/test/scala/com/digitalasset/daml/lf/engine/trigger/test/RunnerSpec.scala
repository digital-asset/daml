// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package test

import akka.stream.scaladsl.{Flow, Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.scalatest.AsyncForAll
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.immutable.Seq
import scala.concurrent.duration.Duration.Zero
import scala.concurrent.Future

class RunnerSpec extends AsyncWordSpec with Matchers with AsyncForAll with AkkaBeforeAndAfterAll {
  import Runner.retrying

  "retrying" should {
    import Future.{successful => okf}

    def runItThrough[A, B](xs: Seq[A])(flow: Flow[A, B, _]): Future[Seq[B]] =
      Source(xs).via(flow).runWith(Sink.seq)

    "terminate immediately on empty input" in {
      Source
        .empty[Unit]
        .via(retrying(5, _ => Zero, 8, a => okf(Some(a)), okf))
        .runWith(Sink.seq)
        .map(_ shouldBe empty)
    }

    "ignore retryable function if no retries" in forAllAsync(trials = 5) { xs: Seq[Int] =>
      runItThrough(xs)(retrying(1, _ => Zero, 8, _ => fail("retried"), a => okf(a + 42)))
        .map(_ should ===(xs map (_ + 42)))
    }

    "not retry if retryable succeeded" in forAllAsync(trials = 5) { xs: Seq[Int] =>
      runItThrough(xs)(retrying(2, _ => Zero, 8, a => okf(Some(a + 42)), a => okf(a - 42)))
        .map(_ should ===(xs map (_ + 42)))
    }
  }
}
