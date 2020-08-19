// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import java.time
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class RetryStrategySpec extends AsyncWordSpec with Matchers {

  "RetryStrategy.constant" should {
    "try a number of times, with a constant delay" in {
      val retry = RetryStrategy.constant(attempts = 10, waitTime = 10.milliseconds)
      val retryCount = new AtomicInteger()
      val start = time.Instant.now()
      for {
        result <- retry { (_, _) =>
          Future {
            if (retryCount.incrementAndGet() >= 5) {
              "Success!"
            } else {
              throw new IllegalStateException("Failure!")
            }
          }
        }
        end = time.Instant.now()
      } yield {
        result should be("Success!")
        retryCount.get() should be(5)
        time.Duration.between(start, end).toMillis should (be >= 50L and be <= 250L)
      }
    }

    "fail if the number of attempts is exceeded" in {
      val retry = RetryStrategy.constant(attempts = 10, waitTime = 10.milliseconds)
      val retryCount = new AtomicInteger()
      val start = time.Instant.now()
      for {
        result <- retry { (_, _) =>
          Future {
            if (retryCount.incrementAndGet() > 11) {
              "Success!"
            } else {
              throw new IllegalStateException("Failure!")
            }
          }
        }.failed
        end = time.Instant.now()
      } yield {
        result should be(an[IllegalStateException])
        result.getMessage should be("Failure!")
        retryCount.get() should be(11)
        time.Duration.between(start, end).toMillis should (be >= 100L and be <= 500L)
      }
    }
  }

  "RetryStrategy.exponentialBackoff" should {
    "try a number of times, with an exponentially-increasing delay" in {
      val retry = RetryStrategy.exponentialBackoff(attempts = 10, firstWaitTime = 10.milliseconds)
      val retryCount = new AtomicInteger()
      val start = time.Instant.now()
      for {
        result <- retry { (_, _) =>
          Future {
            if (retryCount.incrementAndGet() >= 5) {
              "Success!"
            } else {
              throw new IllegalStateException("Failure!")
            }
          }
        }
        end = time.Instant.now()
      } yield {
        result should be("Success!")
        retryCount.get() should be(5)
        time.Duration.between(start, end).toMillis should (be >= 150L and be <= 250L)
      }
    }

    "fail if the number of attempts is exceeded" in {
      val retry = RetryStrategy.exponentialBackoff(attempts = 5, firstWaitTime = 10.milliseconds)
      val retryCount = new AtomicInteger()
      val start = time.Instant.now()
      for {
        result <- retry { (_, _) =>
          Future {
            if (retryCount.incrementAndGet() > 6) {
              "Success!"
            } else {
              throw new IllegalStateException("Failure!")
            }
          }
        }.failed
        end = time.Instant.now()
      } yield {
        result should be(an[IllegalStateException])
        result.getMessage should be("Failure!")
        retryCount.get() should be(6)
        time.Duration.between(start, end).toMillis should (be >= 310L and be <= 500L)
      }
    }
  }

}
