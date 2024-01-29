// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import scala.concurrent.{ExecutionContext, Future => ScalaFuture}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

object Delayed {

  def by[T](t: Duration)(value: => T)(implicit ec: ExecutionContext): ScalaFuture[T] =
    Future.by(t)(ScalaFuture(value))

  object Future {
    def by[T](t: Duration)(value: => ScalaFuture[T]): ScalaFuture[T] =
      if (!t.isFinite) {
        ScalaFuture.failed(new IllegalArgumentException(s"A task cannot be postponed indefinitely"))
      } else if (t.length < 1) {
        try value
        catch { case NonFatal(e) => ScalaFuture.failed(e) }
      } else {
        val task = new PromiseTask(value)
        Timer.schedule(task, t.toMillis)
        task.future
      }
  }

}
