// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import com.digitalasset.canton.ScalaFuturesWithPatience
import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{Await, Promise}

class BaseDriverTest
    extends AnyWordSpec
    with Matchers
    with LazyLogging
    with ScalaFuturesWithPatience {

  implicit val actorSystem: ActorSystem = ActorSystem()

  "bot graph" should {

    "send transactions in order and flush properly" in {

      val source = Source(1 to 1000)
      val promise = Promise[Unit]()
      val counter = new AtomicInteger(1)
      val bot = new BaseDriver.Flusher[Int] {

        def name: String = "test"

        override def update(transaction: List[Int]): Boolean = {
          transaction.foreach { x =>
            if (counter.get() != x) {
              logger.error(s"Expected ${counter.get}, observed $x")
            } else {
              counter.set(x + 1)
            }
          }
          true
        }
        override def flush(): Boolean = {
          if (counter.get() > 1000)
            promise.trySuccess(())
          false
        }
        override def isActive: Boolean = true
      }
      val graph = BaseDriver.buildGraph[Int](bot.driver())

      val completed = source.via(graph).run()

      Await.result(completed, 10.seconds)
      assertResult(1001)(counter.get)

    }
  }

}
