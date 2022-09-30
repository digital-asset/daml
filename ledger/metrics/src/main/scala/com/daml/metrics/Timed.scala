// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.scaladsl.{Keep, Source}
import com.codahale.{metrics => codahale}
import com.daml.metrics.MetricHandle.{Counter, Meter, Timer}
import com.daml.concurrent

import scala.concurrent.{ExecutionContext, Future}

object Timed {

  def value[T](timer: Timer, value: => T): T =
    this.value[T](timer.metric, value)

  def value[T](timer: codahale.Timer, value: => T): T =
    timer.time(() => value)

  def trackedValue[T](meter: Meter, value: => T): T = {
    meter.metric.mark(+1)
    val result = value
    meter.metric.mark(-1)
    result
  }

  def timedAndTrackedValue[T](timer: Timer, meter: Meter, value: => T): T = {
    Timed.value(timer, trackedValue(meter, value))
  }

  def completionStage[T](timer: Timer, future: => CompletionStage[T]): CompletionStage[T] = {
    val ctx = timer.time()
    future.whenComplete { (_, _) =>
      ctx.stop()
      ()
    }
  }

  def trackedCompletionStage[T](meter: Meter, future: => CompletionStage[T]): CompletionStage[T] = {
    meter.metric.mark(+1)
    future.whenComplete { (_, _) =>
      meter.metric.mark(-1)
      ()
    }
  }

  def timedAndTrackedCompletionStage[T](
      timer: Timer,
      meter: Meter,
      future: => CompletionStage[T],
  ): CompletionStage[T] = {
    Timed.completionStage(timer, trackedCompletionStage(meter, future))
  }

  def future[T](timer: Timer, future: => Future[T]): Future[T] = {
    this.future[T](timer.metric, future)
  }

  def future[T](timer: codahale.Timer, future: => Future[T]): Future[T] = {
    val ctx = timer.time()
    val result = future
    result.onComplete(_ => ctx.stop())(ExecutionContext.parasitic)
    result
  }

  def future[EC, T](timer: Timer, future: => concurrent.Future[EC, T]): concurrent.Future[EC, T] = {
    val ctx = timer.time()
    val result = future
    result.onComplete(_ => ctx.stop())(concurrent.ExecutionContext.parasitic)
    result
  }

  def trackedFuture[T](counter: Counter, future: => Future[T]): Future[T] = {
    counter.metric.inc()
    future.andThen { case _ => counter.metric.dec() }(ExecutionContext.parasitic)
  }

  def trackedFuture[T](meter: Meter, future: => Future[T]): Future[T] = {
    meter.metric.mark(+1)
    future.andThen { case _ => meter.metric.mark(-1) }(ExecutionContext.parasitic)
  }

  def timedAndTrackedFuture[T](timer: Timer, counter: Counter, future: => Future[T]): Future[T] = {
    Timed.future(timer, trackedFuture(counter, future))
  }

  def timedAndTrackedFuture[T](timer: Timer, meter: Meter, future: => Future[T]): Future[T] = {
    Timed.future(timer, trackedFuture(meter, future))
  }

  def source[Out, Mat](timer: Timer, source: => Source[Out, Mat]): Source[Out, Mat] = {
    val ctx = timer.time()
    source
      .watchTermination()(Keep.both[Mat, Future[Done]])
      .mapMaterializedValue { case (mat, done) =>
        done.onComplete(_ => ctx.stop())(ExecutionContext.parasitic)
        mat
      }
  }

}
