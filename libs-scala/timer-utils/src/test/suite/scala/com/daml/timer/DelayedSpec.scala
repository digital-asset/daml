// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import java.time

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, DurationInt}

class DelayedSpec extends AsyncWordSpec with Matchers {

  "Delayed.Future" should {
    "delay by zero" in {
      for {
        result <- Delayed.Future.by(Duration.Zero)(Future {
          7
        })
      } yield {
        result should be(7)
      }
    }

    "delay by a little while" in {
      val start = time.Instant.now()
      for {
        result <- Delayed.Future.by(1.second)(Future {
          10
        })
        end = time.Instant.now()
      } yield {
        result should be(10)
        time.Duration.between(start, end).toMillis should (be >= 1000L and be <= 1500L)
      }
    }

    "delay by zero if a negative duration is provided" in {
      val start = time.Instant.now()
      for {
        result <- Delayed.Future.by(-1.second)(Future {
          12
        })
        end = time.Instant.now()
      } yield {
        result should be(12)
        time.Duration.between(start, end).toMillis should (be < 500L)
      }
    }

    "rethrow a failed future" in {
      for {
        exception <- Delayed.Future
          .by(10.milliseconds)(Future {
            throw new IllegalStateException("Whoops.")
          })
          .failed
      } yield {
        exception should be(an[IllegalStateException])
        exception.getMessage should be("Whoops.")
      }
    }

    "throw an error if the duration is not finite" in {
      for {
        exception <- Delayed.Future
          .by(Duration.Inf)(Future {
            14
          })
          .failed
      } yield {
        exception should be(an[IllegalArgumentException])
      }
    }
  }

}
