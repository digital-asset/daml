// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.{BaseTest, config}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Timer
import scala.concurrent.Promise
import scala.util.Failure

class CancellableTimeoutSupportSpec
    extends AnyFlatSpec
    with Matchers
    with Eventually
    with ScalaFutures
    with BaseTest {

  behavior of classOf[CancellableTimeoutSupport].getSimpleName

  it should "schedule a command entry task" in new TestFixture {
    override def run(): Unit = {
      val timeoutDuration = config.NonNegativeFiniteDuration.ofMillis(10L)
      val exception = new RuntimeException("on failure")
      val failure = Failure(exception)
      val promise = Promise[String]()

      cancellableTimeoutSupport.scheduleOnce(
        timeoutDuration,
        promise = promise,
        onTimeout = failure,
      )

      promise.future.failed.futureValue shouldBe exception
    }
  }

  it should "cancel a scheduled task on close" in new TestFixture {
    override def run(): Unit = {
      val timeoutDuration = config.NonNegativeFiniteDuration.ofMillis(10L)
      val exception = new RuntimeException("on failure")
      val failure = Failure(exception)
      val promise = Promise[String]()

      val scheduled = cancellableTimeoutSupport.scheduleOnce(
        timeoutDuration,
        promise = promise,
        onTimeout = failure,
      )

      scheduled.close()
      // Check that the task hasn't executed after the timeout duration expired
      Threading.sleep(1000L)
      promise.isCompleted shouldBe false
    }
  }

  private trait TestFixture {
    def run(): Unit

    private val timer = new Timer("test-timer")
    val cancellableTimeoutSupport = new CancellableTimeoutSupportImpl(timer, loggerFactory)
    run()
    timer.cancel()
  }
}
