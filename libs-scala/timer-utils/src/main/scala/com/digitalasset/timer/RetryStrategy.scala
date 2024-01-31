// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import com.daml.timer.RetryStrategy._

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration, SECONDS}
import scala.concurrent.{ExecutionContext, Future}

object RetryStrategy {

  /** Retry a fixed amount of times with exponential backoff, regardless of the exception thrown
    */
  def exponentialBackoff(attempts: Int, firstWaitTime: Duration): RetryStrategy =
    new RetryStrategy(
      Some(attempts),
      firstWaitTime,
      firstWaitTime * math.pow(2.0, attempts.toDouble),
      _ * 2,
      { case _ => true },
    )

  /** Retry a fixed amount of times with constant wait time, regardless of the exception thrown
    */
  def constant(attempts: Int, waitTime: Duration): RetryStrategy =
    new RetryStrategy(Some(attempts), waitTime, waitTime, identity, { case _ => true })

  /** Retry with constant wait time, but only if the exception satisfies a predicate
    */
  def constant(attempts: Option[Int] = None, waitTime: Duration)(
      predicate: PartialFunction[Throwable, Boolean]
  ): RetryStrategy =
    new RetryStrategy(attempts, waitTime, waitTime, identity, predicate)

  sealed abstract class RetryStrategyException(message: String, cause: Throwable)
      extends RuntimeException(message, cause) {
    def this(message: String) = this(message, null)
  }

  final case object ZeroAttemptsException
      extends RetryStrategyException("Cannot retry an operation with zero or negative attempts.")

  sealed abstract class FailedRetryException(message: String, cause: Throwable)
      extends RetryStrategyException(message, cause)

  object FailedRetryException {
    def unapply(exception: Throwable): Option[Throwable] = exception match {
      case exception: FailedRetryException => Some(exception.getCause)
      case _ => None
    }
  }

  final case class TooManyAttemptsException(
      attempts: Int,
      duration: FiniteDuration,
      message: String,
      cause: Throwable,
  ) extends FailedRetryException(message, cause)

  final case class UnhandledFailureException(
      duration: FiniteDuration,
      message: String,
      cause: Throwable,
  ) extends FailedRetryException(message, cause)

}

final class RetryStrategy private (
    attempts: Option[Int],
    firstWaitTime: Duration,
    waitTimeCap: Duration,
    progression: Duration => Duration,
    predicate: PartialFunction[Throwable, Boolean],
) {

  private def clip(t: Duration): Duration = t.min(waitTimeCap).max(0.millis)

  /** Retries `run` until:
    * - obtaining a successful future,
    * - or retry strategy gave up re-trying.
    */
  def apply[A](run: (Int, Duration) => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    val startTime = System.nanoTime()
    if (attempts.exists(_ <= 0)) {
      Future.failed(ZeroAttemptsException)
    } else {
      def go(attempt: Int, wait: Duration): Future[A] = {
        run(attempt, wait).recoverWith { case throwable =>
          if (attempts.exists(attempt >= _)) {
            val timeTaken = Duration.fromNanos(System.nanoTime() - startTime)
            val message =
              s"Gave up trying after $attempts attempts and ${timeTaken.toUnit(SECONDS)} seconds."
            Future.failed(TooManyAttemptsException(attempt, timeTaken, message, throwable))
          } else if (predicate.lift(throwable).getOrElse(false)) {
            Delayed.Future.by(wait)(go(attempt + 1, clip(progression(wait))))
          } else {
            val timeTaken = Duration.fromNanos(System.nanoTime() - startTime)
            val message =
              s"Gave up trying due to an unhandled failure after ${timeTaken.toUnit(SECONDS)} seconds."
            Future.failed(UnhandledFailureException(timeTaken, message, throwable))
          }
        }
      }

      go(1, clip(firstWaitTime))
    }
  }

}
