// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricHandle.{Counter, Timer}
import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.{Keep, Source}

import scala.concurrent.{ExecutionContext, Future}

object Timed {

  def value[T](timer: Timer, value: => T): T =
    timer.time(value)

  def future[T](timer: Timer, future: => Future[T]): Future[T] =
    timer.timeFuture(future)

  def timedAndTrackedFuture[T](timer: Timer, counter: Counter, future: => Future[T]): Future[T] =
    Timed.future(timer, Tracked.future(counter, future))

  /** Be advised that this will time the source when it's created and not when it's actually run.
    */
  def source[Out, Mat](timer: Timer, source: => Source[Out, Mat]): Source[Out, Mat] = {
    val timerHandle = timer.startAsync()
    source
      .watchTermination()(Keep.both[Mat, Future[Done]])
      .mapMaterializedValue { case (mat, done) =>
        done.onComplete(_ => timerHandle.stop())(ExecutionContext.parasitic)
        mat
      }
  }

}
