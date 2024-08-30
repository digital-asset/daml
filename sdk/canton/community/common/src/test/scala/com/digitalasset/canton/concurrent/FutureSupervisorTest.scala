// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future
import scala.util.Success

trait FutureSupervisorTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  def futureSupervisor(supervisor: FutureSupervisor): Unit = {

    "support a lot of concurrent supervisions" in {
      // Ensure that there is no quadratic algorithm anywhere in the supervision code
      val count = 1000000

      val supervised = (1 to count).toList.map { i =>
        futureSupervisor.supervised(s"test-$i")(Future.successful(i))
      }
      Future.sequence(supervised).futureValue
    }

    "supervising a completed promise's future is a no-op" in {
      val promise = new SupervisedPromise[Unit]("to be completed immediately", supervisor)

      promise.trySuccess(())
      promise.future.value shouldBe Some(Success(()))
    }

    "repeated calls to supervised promises are cached" in {
      val promise = new SupervisedPromise[CantonTimestamp]("future is cached", supervisor)

      val fut1 = promise.future
      val fut2 = promise.future

      fut1 shouldBe fut2

      val timestamp = CantonTimestamp.now()
      promise.trySuccess(timestamp)

      val fut3 = promise.future

      fut3 shouldBe fut1
      fut3.value shouldBe Some(Success(timestamp))
    }
  }
}

class NoOpFutureSupervisorTest extends FutureSupervisorTest {
  "NoOpFutureSupervisor" should {
    behave like futureSupervisor(FutureSupervisor.Noop)
  }
}

class FutureSupervisorImplTest extends FutureSupervisorTest {

  "FutureSupervisorImpl" should {
    behave like futureSupervisor(
      new FutureSupervisor.Impl(config.NonNegativeDuration.ofSeconds(10))(scheduledExecutor())
    )
  }
}
