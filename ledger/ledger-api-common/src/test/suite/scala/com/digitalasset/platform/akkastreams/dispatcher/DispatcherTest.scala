// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.akkastreams.dispatcher

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContextExecutor, Future}

//TODO: merge/review the tests we have around the Dispatcher!
class DispatcherTest
    extends AnyWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(10, Seconds)), scaled(Span(250, Milliseconds)))

  "A Dispatcher" should {
    "not race when creating new subscriptions" in {
      // The test setup here is a little different from the above tests,
      // because we wanted to be specific about emitted pairs and use of Thread.sleep.

      implicit val ec: ExecutionContextExecutor = materializer.executionContext

      val elements = new AtomicReference(Map.empty[Int, Int])
      def readElement(i: Int): Future[Int] = Future {
        Thread.sleep(10) // In a previous version of Dispatcher, this sleep caused a race condition.
        elements.get()(i)
      }
      def readSuccessor(i: Int): Int = i + 1

      // compromise between catching flakes and not taking too long
      0 until 25 foreach { _ =>
        val d = Dispatcher("test", 0, 0)

        // Verify that the results are what we expected
        val subscriptions = 1 until 10 map { i =>
          elements.updateAndGet(m => m + (i -> i))
          d.signalNewHead(i)
          d.startingAt(i - 1, OneAfterAnother(readSuccessor, readElement))
            .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[(Int, Int)]]])
            .run()
        }

        d.shutdown()

        subscriptions.zip(1 until 10) foreach { case (f, i) =>
          whenReady(f) { vals =>
            vals.map(_._1) should contain theSameElementsAs (i to 9)
            vals.map(_._2) should contain theSameElementsAs (i until 10)
          }
        }
      }
    }
  }
}
