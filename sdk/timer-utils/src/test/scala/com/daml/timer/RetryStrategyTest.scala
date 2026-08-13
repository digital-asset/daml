// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class RetryStrategyTest extends AsyncWordSpec with Matchers {

  "RetryStrategy.exponentialBackoff" should {
    "not throw overflow exception when attempts count is large" in {
      noException should be thrownBy {
        RetryStrategy.exponentialBackoff(attempts = 1024, firstWaitTime = 10.millis)
      }
      succeed
    }

    "format attempts count string cleanly without printing Option wrapper" in {
      val strategy = RetryStrategy.exponentialBackoff(attempts = 1, firstWaitTime = 1.milli)
      strategy { (_, _) =>
        scala.concurrent.Future.failed(new RuntimeException("test error"))
      }.failed.map {
        case ex: RetryStrategy.TooManyAttemptsException =>
          ex.getMessage should include("after 1 attempts")
          ex.getMessage should not include "Some(1)"
        case other =>
          fail(s"Unexpected exception type: $other")
      }
    }
  }
}
