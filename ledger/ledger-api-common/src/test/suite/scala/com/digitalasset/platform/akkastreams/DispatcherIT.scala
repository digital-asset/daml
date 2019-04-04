// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.akkastreams

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.{ExecutionContextExecutor, Future}

class DispatcherIT extends WordSpec with AkkaBeforeAndAfterAll with Matchers with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(10, Seconds)), scaled(Span(250, Milliseconds)))

  "A Dispatcher" should {
    "not race when creating new subscriptions" in {
      // The test setup here is a little different from the above tests,
      // because we wanted to be specific about emitted pairs and use of Thread.sleep.

      implicit val ec: ExecutionContextExecutor = materializer.executionContext

      val head = new AtomicInteger(0)
      val elements = new AtomicReference(Map.empty[Int, Int])
      def readElement(i: Int): Future[Int] = Future {
        Thread.sleep(10) // In a previous version of Dispatcher, this sleep caused a race condition.
        elements.get()(i)
      }
      def readSuccessor(i: Int, _v: Int): Int = i + 1

      // compromise between catching flakes and not taking too long
      0 until 25 foreach { _ =>
        val d = Dispatcher(readSuccessor, readElement, 0, 0)
        head.set(0)

        // Verify that the results are what we expected
        val subscriptions = 0 until 10 map { i =>
          elements.updateAndGet(m => m + (i -> i))
          head.set(i + 1)
          d.signalNewHead(i + 1)
          d.startingAt(i).toMat(Sink.collection)(Keep.right[NotUsed, Future[Seq[(Int, Int)]]]).run()
        }

        d.close()

        subscriptions.zipWithIndex foreach {
          case (f, i) =>
            whenReady(f) { vals =>
              vals.map(_._1) should contain theSameElementsAs (i + 1 to 10)
              vals.map(_._2) should contain theSameElementsAs (i until 10)
            }
        }
      }
    }
  }
}
