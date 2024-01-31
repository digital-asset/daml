// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import java.util.TimerTask

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration

object Timeout {

  /** Creates a new Future with specifying a timeout.
    * A Future cannot be stopped once started. With this utility the resulting Future will finish with a different result if the timeout is reached, with the subsequent result of the original Future being discarded.
    * Important note: the original future will always run until completion, even if the timeout is reached.
    *
    * @param duration The timeout duration: if infinite, no timeout will happen.
    * @param onTimeout A computation resulting in a value of type T or an exception.
    *                  The value will be computed when the timeout is reached.
    *                  If it is computed, it will determine the value of the result Future.
    * @param f The original future.
    * @return either the result of the original (if it completes in time),
    *         or the result of the computation specified for onTimeout.
    *         If onTimeout is computed, it will be the result as well.
    */
  def apply[T](duration: Duration)(onTimeout: => T)(f: Future[T]): Future[T] =
    if (duration.isFinite) {
      val p = Promise[Option[T]]()
      val timeoutTask = new TimerTask {
        override def run(): Unit = {
          p.trySuccess(None)
          ()
        }
      }
      Timer.schedule(timeoutTask, duration.toMillis)
      f.onComplete { result =>
        if (p.tryComplete(result.map(Some(_)))) timeoutTask.cancel()
        ()
      }(scala.concurrent.ExecutionContext.parasitic)
      p.future.map {
        case None => onTimeout
        case Some(result) => result
      }(scala.concurrent.ExecutionContext.parasitic)
    } else {
      f
    }

  implicit class FutureTimeoutOps[T](val f: Future[T]) extends AnyVal {
    def withTimeout(duration: Duration)(onTimeout: => T): Future[T] =
      Timeout(duration)(onTimeout)(f)
  }
}
