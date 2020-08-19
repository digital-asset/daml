// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import java.time
import java.util.concurrent.atomic.AtomicInteger

import com.daml.timer.RetryStrategySpec._
import org.scalatest.{AsyncWordSpec, Inside, Matchers}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class RetryStrategySpec extends AsyncWordSpec with Matchers with Inside {

  "RetryStrategy.constant" should {
    "try a number of times, with a constant delay" in {
      val retry = RetryStrategy.constant(attempts = 10, waitTime = 10.milliseconds)
      for {
        (retryCount, result, duration) <- succeedAfter(tries = 5, retry)
      } yield {
        result should be(Success("Success!"))
        retryCount should be(5)
        duration.toMillis should (be >= 50L and be <= 250L)
      }
    }

    "fail if the number of attempts is exceeded" in {
      val retry = RetryStrategy.constant(attempts = 10, waitTime = 10.milliseconds)
      for {
        (retryCount, result, duration) <- succeedAfter(tries = 12, retry)
      } yield {
        inside(result) {
          case Failure(_: FailureDuringRetry) =>
        }
        retryCount should be(11)
        duration.toMillis should (be >= 100L and be <= 500L)
      }
    }
  }

  "RetryStrategy.exponentialBackoff" should {
    "try a number of times, with an exponentially-increasing delay" in {
      val retry = RetryStrategy.exponentialBackoff(attempts = 10, firstWaitTime = 10.milliseconds)
      for {
        (retryCount, result, duration) <- succeedAfter(tries = 5, retry)
      } yield {
        result should be(Success("Success!"))
        retryCount should be(5)
        duration.toMillis should (be >= 150L and be <= 250L)
      }
    }

    "fail if the number of attempts is exceeded" in {
      val retry = RetryStrategy.exponentialBackoff(attempts = 5, firstWaitTime = 10.milliseconds)
      for {
        (retryCount, result, duration) <- succeedAfter(tries = 7, retry)
      } yield {
        inside(result) {
          case Failure(_: FailureDuringRetry) =>
        }
        retryCount should be(6)
        duration.toMillis should (be >= 310L and be <= 500L)
      }
    }
  }

}

object RetryStrategySpec {

  private def succeedAfter(
      tries: Int,
      retry: RetryStrategy,
  )(implicit executionContext: ExecutionContext): Future[(Int, Try[String], time.Duration)] = {
    val retryCount = new AtomicInteger()
    val start = time.Instant.now()
    retry { (_, _) =>
      Future {
        if (retryCount.incrementAndGet() >= tries) {
          "Success!"
        } else {
          throw new FailureDuringRetry
        }
      }
    }.transform(Success(_))
      .map((retryCount.get(), _, time.Duration.between(start, time.Instant.now())))
  }

  private final class FailureDuringRetry extends RuntimeException

}
