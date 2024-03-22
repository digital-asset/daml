// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.timer.RetryStrategy.TooManyAttemptsException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class EventuallySpec extends AsyncWordSpec with Matchers {

  "eventually" should {
    "enhance the exception message with the assertion name" in {
      recoverToExceptionIf[TooManyAttemptsException] {
        eventually(assertionName = "test", attempts = 1, firstWaitTime = 0.millis) {
          Future.failed(new RuntimeException())
        }
      }.map(_.message should startWith("test: "))
    }
  }
}
