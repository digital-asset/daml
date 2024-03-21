// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}

class TimeoutSpec extends AsyncFlatSpec with Matchers {
  import Timeout._
  // in order to have a multi-threaded EC in scope
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  behavior of "Futures with Timeout"

  it should "time out, and result should be taken from onTimeout" in {
    val resultPromise = Promise[Int]()
    Future {
      Thread.sleep(100)
      10
    }
      .withTimeout(FiniteDuration(50, "millis"))(5)
      .onComplete(resultPromise.tryComplete)
    Thread.sleep(25)
    resultPromise.isCompleted shouldBe false
    Thread.sleep(50)
    resultPromise.isCompleted shouldBe true
    resultPromise.future.map {
      _ shouldBe 5
    }
  }

  it should "time out, and exception should be taken from onTimeout" in {
    val resultPromise = Promise[Int]()
    Future {
      Thread.sleep(100)
      10
    }
      .withTimeout(FiniteDuration(50, "millis"))(throw new Exception("boom"))
      .onComplete(resultPromise.tryComplete)
    Thread.sleep(25)
    resultPromise.isCompleted shouldBe false
    Thread.sleep(50)
    resultPromise.isCompleted shouldBe true
    resultPromise.future.failed.map {
      _.getMessage shouldBe "boom"
    }
  }

  it should "not time out if timeout is not reached, and onTimeout should not be executed" in {
    val resultPromise = Promise[Int]()
    val timeoutPromise = Promise[Unit]()
    Future {
      Thread.sleep(25)
      10
    }
      .withTimeout(FiniteDuration(50, "millis")) {
        timeoutPromise.trySuccess(())
        throw new Exception("boom")
      }
      .onComplete(resultPromise.tryComplete)
    Thread.sleep(10)
    resultPromise.isCompleted shouldBe false
    Thread.sleep(50)
    resultPromise.isCompleted shouldBe true
    resultPromise.future.map { result =>
      result shouldBe 10
      timeoutPromise.isCompleted shouldBe false
    }
  }

  it should "not time out if timeout is infinite" in {
    val resultPromise = Promise[Int]()
    Future {
      Thread.sleep(25)
      10
    }
      .withTimeout(Duration.Inf)(throw new Exception("boom"))
      .onComplete(resultPromise.tryComplete)
    Thread.sleep(10)
    resultPromise.isCompleted shouldBe false
    Thread.sleep(50)
    resultPromise.isCompleted shouldBe true
    resultPromise.future.map {
      _ shouldBe 10
    }
  }

}
