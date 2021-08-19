// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.http.util.Concurrent._
import scalaz.Scalaz._

import scala.concurrent.{Future, blocking}

class ConcurrentTest extends AsyncFlatSpec with Matchers {

  @volatile var occupied = false
  @volatile var counter = 0

  it should "be nice" in {
    Logging.instanceUUIDLogCtx { implicit lc =>
      val mutex = Mutex()
      def test() =
        List
          .tabulate(10) { _ =>
            lazy val ifAvailable = Future {
              if (occupied)
                throw new RuntimeException("Already occupied, this shouldn't happen")
              else occupied = true
              counter += 1
              blocking {
                Thread.sleep(400)
              }
              occupied = false
            }
            Future {
              blocking {
                Thread.sleep(400)
              }
            }.flatMap(_ => executeIfAvailable(mutex)(ifAvailable)(Future.unit))
          }
          .sequence
      for {
        _ <- test()
        _ = counter should be > 0
        previousCounterValue = counter
        _ <- test()
        _ = counter should be > previousCounterValue
        previousCounterValue2 = counter
        _ <- test()
        _ = counter should be > previousCounterValue2
      } yield succeed

    }
  }
}
