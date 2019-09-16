// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.{Timer, TimerTask}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.duration.DurationInt
import scala.util.Try
import scala.util.control.NonFatal

object RetryStrategy {

  private[this] val timer: Timer = new Timer("retry-scheduler", true)

  private class PromiseTask[A](value: => Future[A]) extends TimerTask with Promise[A] {

    private[this] val p = Promise[A]

    override def run(): Unit = {
      p.tryCompleteWith {
        try value
        catch { case NonFatal(t) => Future.failed(t) }
      }
    }

    override def future: Future[A] = p.future

    override def isCompleted: Boolean = p.isCompleted

    override def tryComplete(result: Try[A]): Boolean = p.tryComplete(result)

  }

  private def after[T](t: Duration)(value: => Future[T])(implicit ec: ExecutionContext): Future[T] =
    if (!t.isFinite) {
      Future.failed(new IllegalArgumentException(s"Cannot schedule task after $t"))
    } else if (t.length < 1) {
      try value
      catch { case NonFatal(e) => Future.failed(e) }
    } else {
      val task = new PromiseTask(value)
      timer.schedule(task, t.toMillis)
      task.future
    }

  def exponentialBackoff(attempts: Int, firstWaitTime: Duration): RetryStrategy =
    new RetryStrategy(
      attempts,
      firstWaitTime,
      firstWaitTime * math.pow(2.0, attempts.toDouble),
      _ * 2)

  def constant(attempts: Int, waitTime: Duration): RetryStrategy =
    new RetryStrategy(attempts, waitTime, waitTime, identity)

}

final class RetryStrategy(
    attempts: Int,
    firstWaitTime: Duration,
    waitTimeCap: Duration,
    progression: Duration => Duration) {
  import RetryStrategy.after
  private def clip(t: Duration): Duration = t.min(waitTimeCap).max(0.millis)
  def apply[A](run: Int => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    def go(attempt: Int, wait: Duration): Future[A] = {
      run(attempt)
        .recoverWith {
          case throwable if attempt > attempts =>
            Future.failed(throwable)
          case _ =>
            after(wait)(go(attempt + 1, clip(progression(wait))))
        }
    }
    go(1, clip(firstWaitTime))
  }
}
