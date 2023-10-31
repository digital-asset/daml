// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.FlagCloseable
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.*

class DelayUtilTest extends AnyWordSpec with BaseTest {
  "DelayUtil.delay" should {
    "succeed roughly within the given delay" in {
      val delay = 100.millis
      val deadline = delay.fromNow
      Await.result(DelayUtil.delay(delay), Duration.Inf)

      deadline.isOverdue() shouldBe true
      -deadline.timeLeft should be < delay * 10
    }

    "not prevent termination" in {
      val executorService =
        Threading.singleThreadScheduledExecutor("delay-util-test-executor", noTracingLogger)

      val delayed = DelayUtil.delay(executorService, 1.minute, _.success(()))

      // Executor service terminates immediately despite a pending task.
      executorService.shutdown()
      executorService.awaitTermination(1, TimeUnit.SECONDS) shouldBe true
      delayed.isCompleted shouldBe false
    }

    "not schedule when already closing" in {
      val flagCloseable =
        FlagCloseable(DelayUtilTest.this.logger, DefaultProcessingTimeouts.testing)

      val delayedCloseable = DelayUtil.delay("test", 20.millis, flagCloseable)
      flagCloseable.close()
      Threading.sleep(100)
      assert(!delayedCloseable.isCompleted, "Future completed during shutdown")
    }
  }
}
