// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object RetryStrategy {

  /**
    * Retry a fixed amount of times with exponential backoff, regardless of the exception thrown
    *
    */
  def exponentialBackoff(attempts: Int, firstWaitTime: Duration): RetryStrategy =
    new RetryStrategy(
      Some(attempts),
      firstWaitTime,
      firstWaitTime * math.pow(2.0, attempts.toDouble),
      _ * 2,
      { case _ => true }
    )

  /**
    * Retry a fixed amount of times with constant wait time, regardless of the exception thrown
    *
    */
  def constant(attempts: Int, waitTime: Duration): RetryStrategy =
    new RetryStrategy(Some(attempts), waitTime, waitTime, identity, { case _ => true })

  /**
    * Retry with constant wait time, but only if the exception satisfies a predicate
    */
  def constant(attempts: Option[Int] = None, waitTime: Duration)(
      predicate: PartialFunction[Throwable, Boolean]
  ): RetryStrategy =
    new RetryStrategy(attempts, waitTime, waitTime, identity, predicate)
}

final class RetryStrategy private (
    attempts: Option[Int],
    firstWaitTime: Duration,
    waitTimeCap: Duration,
    progression: Duration => Duration,
    predicate: PartialFunction[Throwable, Boolean]) {
  private def clip(t: Duration): Duration = t.min(waitTimeCap).max(0.millis)
  def apply[A](run: (Int, Duration) => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    def go(attempt: Int, wait: Duration): Future[A] = {
      run(attempt, wait)
        .recoverWith {
          case NonFatal(throwable) if attempts.exists(attempt > _) =>
            Future.failed(throwable)
          case NonFatal(throwable) if predicate.lift(throwable).getOrElse(false) =>
            Delayed.Future.by(wait)(go(attempt + 1, clip(progression(wait))))
          case NonFatal(throwable) =>
            Future.failed(throwable)
        }
    }
    go(1, clip(firstWaitTime))
  }
}
