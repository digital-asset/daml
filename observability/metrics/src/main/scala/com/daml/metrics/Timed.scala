// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.CompletionStage

import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import com.daml.concurrent
import com.daml.metrics.api.MetricHandle.{Counter, Timer}

import scala.concurrent.{ExecutionContext, Future}

object Timed {

  def value[T](timer: Timer, value: => T): T =
    timer.time(value)

  def timedAndTrackedValue[T](timer: Timer, counter: Counter, value: => T): T = {
    Timed.value(timer, Tracked.value(counter, value))
  }

  def completionStage[T](timer: Timer, future: => CompletionStage[T]): CompletionStage[T] = {
    val timerHandle = timer.startAsync()
    future.whenComplete { (_, _) =>
      timerHandle.stop()
      ()
    }
  }

  def timedAndTrackedCompletionStage[T](
      timer: Timer,
      counter: Counter,
      future: => CompletionStage[T],
  ): CompletionStage[T] = {
    Timed.completionStage(timer, Tracked.completionStage(counter, future))
  }

  def future[T](timer: Timer, future: => Future[T]): Future[T] = {
    timer.timeFuture(future)
  }

  def future[EC, T](timer: Timer, future: => concurrent.Future[EC, T]): concurrent.Future[EC, T] = {
    val timerHandle = timer.startAsync()
    val result = future
    result.onComplete(_ => timerHandle.stop())(concurrent.ExecutionContext.parasitic)
    result
  }

  def timedAndTrackedFuture[T](timer: Timer, counter: Counter, future: => Future[T]): Future[T] = {
    Timed.future(timer, Tracked.future(counter, future))
  }

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
