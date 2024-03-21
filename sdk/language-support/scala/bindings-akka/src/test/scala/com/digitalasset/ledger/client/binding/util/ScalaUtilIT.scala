// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.util

import java.util.concurrent.{Executors, ScheduledExecutorService}

import com.daml.ledger.client.binding.util.ScalaUtil.FutureOps
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{Future, Promise, TimeoutException}

class ScalaUtilIT
    extends AsyncWordSpec
    with AsyncTimeLimitedTests
    with Matchers
    with BeforeAndAfterAll {

  implicit val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  override def afterAll(): Unit = {
    scheduler.shutdownNow()
    super.afterAll()
  }

  "FutureOps" can {

    "future with timeout" should {

      "fail Future with TimoutException after specified duration" in {
        val promise = Promise[Unit]() // never completes
        val future = promise.future.timeout("name", 1000.millis, 100.millis)
        recoverToSucceededIf[TimeoutException](future)
      }

      "be able to complete within specified duration" in {
        val future = Future {
          "result"
        }.timeoutWithDefaultWarn("name", 1.second)

        future.map(_ shouldBe "result")
      }

    }

  }
  override lazy val timeLimit: Span = 10.seconds
}
