// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import java.util.TimerTask
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object FutureCheck {

  /** Creates a special checker task around passed future to check periodically if it was completed or not.
    * If it was not completed - a passed closure is being called.
    *
    * @param delay  The delay duration before the first check is being done.
    * @param period The duration between checking tasks are being triggered.
    * @param onDeadlineExceeded A closure which will be called in case of future is not yet completed.
    * @param f         The original future.
    */
  def check[T](delay: Duration, period: Duration)(
      f: Future[T]
  )(onDeadlineExceeded: => Unit): Unit =
    Timer.scheduleAtFixedRate(
      task(f, onDeadlineExceeded),
      delay.toMillis,
      period.toMillis,
    )

  /** Creates a special checker task around passed future to check if it was completed or not after delay.
    * If it was not completed - a passed closure is being called.
    *
    * @param delay        The delay duration before the check is done.
    * @param onDeadlineExceeded A closure which will be called in case of future is not yet completed.
    * @param f            The original future.
    */
  def check[T](delay: Duration)(
      f: Future[T]
  )(onDeadlineExceeded: => Unit): Unit = Timer.schedule(
    task(f, onDeadlineExceeded),
    delay.toMillis,
  )

  private def task[T](f: Future[T], onDeadlineExceeded: => Unit): TimerTask =
    new TimerTask {
      override def run(): Unit =
        if (!f.isCompleted) {
          onDeadlineExceeded
        } else {
          val _ = cancel()
        }
    }

  implicit class FutureTimeoutOps[T](val f: Future[T]) extends AnyVal {
    def checkIfComplete(delay: Duration)(onDeadlineExceeded: => Unit): Future[T] = {
      FutureCheck.check(delay)(f)(onDeadlineExceeded)
      f
    }

    def checkIfComplete(delay: Duration, period: Duration)(
        onDeadlineExceeded: => Unit
    ): Future[T] = {
      FutureCheck.check(delay, period)(f)(onDeadlineExceeded)
      f
    }
  }
}
