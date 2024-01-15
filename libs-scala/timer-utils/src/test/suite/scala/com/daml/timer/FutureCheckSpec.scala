// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.timer.FutureCheck._

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.Future
import scala.concurrent.duration._

class FutureCheckSpec extends AsyncWordSpec with Matchers {

  "FutureCheck" should {
    "never be called for completed future" in {
      val flag: AtomicBoolean = new AtomicBoolean(false)
      for {
        result <- Future.successful(1).checkIfComplete(0.millis) { flag.set(true) }
      } yield {
        result shouldBe 1
        flag.get() shouldBe false
      }
    }

    "be called for delayed future" in {
      val flag: AtomicBoolean = new AtomicBoolean(false)
      for {
        result <- Delayed.by(10.millis)(1).checkIfComplete(0.millis) {
          flag.set(true)
        }
      } yield {
        result shouldBe 1
        flag.get() shouldBe true
      }
    }

    "not be called for future which is resolved fast enough" in {
      val flag: AtomicBoolean = new AtomicBoolean(false)
      for {
        result <- Delayed.by(100.millis)(1).checkIfComplete(200.millis) {
          flag.set(true)
        }
      } yield {
        result shouldBe 1
        flag.get() shouldBe false
      }
    }

    "run periodically for late future and cancel when its complete" in {
      val flag: AtomicInteger = new AtomicInteger(0)
      for {
        result <- Delayed.by(100.millis)(1).checkIfComplete(50.millis, 10.millis) {
          val _ = flag.incrementAndGet()
        }
        capturedValue = flag.get()
        _ <- Delayed.by(100.millis)(1)
      } yield {
        result shouldBe 1
        flag.get() should (be >= 3 and be <= 7)
        capturedValue should (be >= 3 and be <= 7)
        capturedValue shouldBe flag.get()
      }
    }
  }
}
