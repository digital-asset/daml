// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pekkostreams.dispatcher

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.{DirectExecutionContext, Threading}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.pekkostreams.dispatcher.SubSource.RangeSource
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, Future, blocking}

// Consider merging/reviewing the tests we have around the Dispatcher!
class DispatcherRaceSpec
    extends AnyWordSpec
    with PekkoBeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with BaseTest {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(10, Seconds)), scaled(Span(250, Milliseconds)))

  "A Dispatcher" should {
    "not race when creating new subscriptions" in {
      // The test setup here is a little different from the above tests,
      // because we wanted to be specific about emitted pairs and use of Thread.sleep.

      implicit val ec: ExecutionContextExecutor = materializer.executionContext

      val elements = new AtomicReference(Map.empty[Int, Int])
      def readElement(i: Int): Future[Int] = Future {
        blocking(
          Threading.sleep(10)
        ) // In a previous version of Dispatcher, this sleep caused a race condition.
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
          d.startingAt(
            i - 1,
            RangeSource((startExclusive, endInclusive) =>
              Source
                .unfoldAsync(readSuccessor(startExclusive)) { index =>
                  if (Ordering[Int].gt(index, endInclusive)) Future.successful(None)
                  else {
                    readElement(index).map { t =>
                      val nextIndex = readSuccessor(index)
                      Some((nextIndex, (index, t)))
                    }(DirectExecutionContext(noTracingLogger))
                  }
                }
            ),
          ).toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[(Int, Int)]]])
            .run()
        }

        d.shutdown().discard

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
