// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import java.time
import java.util.concurrent.atomic.AtomicInteger

import com.daml.timer.RetryStrategySpec._
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final class RetryStrategySpec extends AsyncWordSpec with Matchers with CustomMatchers with Inside {

  "RetryStrategy.constant" should {
    "try a number of times, with a constant delay" in {
      val retry = RetryStrategy.constant(attempts = 10, waitTime = 10.milliseconds)
      for {
        (retryCount, result, duration) <- succeedAfter(tries = 5, retry)
      } yield {
        result should be(Success("Success!"))
        retryCount should be(5)
        duration should beAround(50.milliseconds)
      }
    }

    "try forever, with no delay, to ensure we don't overflow the stack" in {
      val retry = RetryStrategy.constant(attempts = None, waitTime = Duration.Zero) { case _ =>
        true
      }
      for {
        (retryCount, result, _) <- succeedAfter(tries = 10000, retry)
      } yield {
        result should be(Success("Success!"))
        retryCount should be(10000)
      }
    }

    "fail if the number of attempts is exceeded" in {
      val retry = RetryStrategy.constant(attempts = 10, waitTime = 10.milliseconds)
      for {
        (retryCount, result, duration) <- succeedAfter(tries = 11, retry)
      } yield {
        result should matchPattern {
          case Failure(RetryStrategy.TooManyAttemptsException(10, _, _, _: FailureDuringRetry)) =>
        }
        retryCount should be(10)
        duration should beAround(100.milliseconds)
      }
    }

    "fail immediately if zero attempts should be made" in {
      val retry = RetryStrategy.constant(attempts = 0, waitTime = 10.milliseconds)
      for {
        (retryCount, result, _) <- succeedAfter(tries = 1, retry)
      } yield {
        result should matchPattern { case Failure(RetryStrategy.ZeroAttemptsException) => }
        retryCount should be(0)
      }
    }

    "fail immediately if a negative number of attempts should be made" in {
      val retry = RetryStrategy.constant(attempts = -7, waitTime = 10.milliseconds)
      for {
        (retryCount, result, _) <- succeedAfter(tries = 1, retry)
      } yield {
        result should matchPattern { case Failure(RetryStrategy.ZeroAttemptsException) => }
        retryCount should be(0)
      }
    }
  }

  "RetryStrategy.exponentialBackoff" should {
    "try a number of times, with an exponentially-increasing delay" in {
      val retry = RetryStrategy.exponentialBackoff(attempts = 10, firstWaitTime = 10.milliseconds)
      for {
        (retryCount, result, duration) <- succeedAfter(tries = 4, retry)
      } yield {
        result should be(Success("Success!"))
        retryCount should be(4)
        duration should beAround(70.milliseconds)
      }
    }

    "fail if the number of attempts is exceeded" in {
      val retry = RetryStrategy.exponentialBackoff(attempts = 5, firstWaitTime = 10.milliseconds)
      for {
        (retryCount, result, duration) <- succeedAfter(tries = 6, retry)
      } yield {
        result should matchPattern {
          case Failure(RetryStrategy.TooManyAttemptsException(5, _, _, _: FailureDuringRetry)) =>
        }
        retryCount should be(5)
        duration should beAround(150.milliseconds)
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
      if (retryCount.incrementAndGet() >= tries) {
        Future.successful("Success!")
      } else {
        Future.failed(new FailureDuringRetry)
      }
    }.transform(Success(_))
      .map((retryCount.get(), _, time.Duration.between(start, time.Instant.now())))
  }

  private final class FailureDuringRetry extends RuntimeException

  private[RetryStrategySpec] trait CustomMatchers {

    final class AroundDurationMatcher(expectedDuration: Duration) extends Matcher[time.Duration] {
      def apply(left: time.Duration): MatchResult = {
        val actual = left.toMillis
        val lowerBound = expectedDuration.toMillis - 20 // Delays can sometimes be too fast.
        val upperBound = expectedDuration.toMillis * 10 // Tests can run slowly in parallel.
        val result = actual >= lowerBound && actual < upperBound
        MatchResult(
          result,
          s"$actual milliseconds was not around $expectedDuration [$lowerBound, $upperBound)",
          s"$actual milliseconds was around $expectedDuration [$lowerBound, $upperBound)",
        )
      }
    }

    def beAround(expectedDuration: Duration) = new AroundDurationMatcher(expectedDuration)

  }

}
