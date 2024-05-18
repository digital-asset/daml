// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.watchdog

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, PositiveFiniteDuration}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

class WatchdogServiceTest extends AnyWordSpec with BaseTest {
  "WatchdogServiceTest" must {
    "execute checkIsAlive periodically" in {
      var checkCounter = 0
      var killCounter = 0
      val watchdogService = new WatchdogService(
        checkInterval = PositiveFiniteDuration(100.milliseconds),
        checkIsAlive = {
          checkCounter += 1
          true
        },
        killDelay = NonNegativeFiniteDuration(10.milliseconds),
        killAction = {
          killCounter += 1
        },
        loggerFactory = loggerFactory,
        timeouts = timeouts,
      )
      Threading.sleep(1000)
      watchdogService.close()
      checkCounter shouldBe >(1)
      killCounter shouldBe 0
    }
    "kill the service using killAction if it is not alive" in {
      var checkCounter = 0
      var killCounter = 0
      loggerFactory.assertLogsUnorderedOptional(
        {
          val watchdogService = new WatchdogService(
            checkInterval = PositiveFiniteDuration(40.milliseconds),
            checkIsAlive = {
              checkCounter += 1
              false
            },
            killDelay = NonNegativeFiniteDuration(10.milliseconds),
            killAction = {
              killCounter += 1
            },
            loggerFactory = loggerFactory,
            timeouts = timeouts,
          )
          Threading.sleep(1000)
          watchdogService.close()
        },
        (
          LogEntryOptionality.Required,
          _.errorMessage shouldBe "Watchdog detected that the service is not alive. Scheduling to kill the service after a delay of 0.01s.",
        ),
        (
          LogEntryOptionality.Required,
          _.errorMessage shouldBe "Watchdog is killing the service now.",
        ),
      )
      checkCounter shouldBe 1
      killCounter shouldBe 1
    }
  }
}
