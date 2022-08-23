package com.daml.timer

import java.util.TimerTask
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object FutureCheck {

  def check[T](delay: Duration, period: Duration)(
      f: Future[T]
  )(ifIncomplete: => Unit): Unit =
    Timer.scheduleAtFixedRate(
      new TimerTask {
        override def run(): Unit = {
          if (!f.isCompleted) {
            ifIncomplete
          }
        }
      },
      delay.toMillis,
      period.toMillis,
    )

  def check[T](delay: Duration)(
      f: Future[T]
  )(ifIncomplete: => Unit) = Timer.schedule(
    new TimerTask {
      override def run(): Unit = {
        if (!f.isCompleted) {
          ifIncomplete
        }
      }
    },
    delay.toMillis,
  )

  implicit class FutureTimeoutOps[T](val f: Future[T]) extends AnyVal {
    def checkIfComplete(delay: Duration)(ifIncomplete: => Unit): Future[T] = {
      FutureCheck.check(delay)(f)(ifIncomplete)
      f
    }

    def checkIfComplete(delay: Duration, period: Duration)(ifIncomplete: => Unit): Future[T] = {
      FutureCheck.check(delay, period)(f)(ifIncomplete)
      f
    }
  }
}
