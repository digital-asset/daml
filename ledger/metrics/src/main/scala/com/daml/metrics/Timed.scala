// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.scaladsl.{Keep, Source}
import com.codahale.metrics.{Counter, Meter, Timer}
import com.daml.concurrent
import com.daml.dec.DirectExecutionContext

import scala.concurrent.Future

object Timed {

  def value[T](timer: Timer, value: => T): T =
    timer.time(() => value)

  def trackedValue[T](meter: Meter, value: => T): T = {
    meter.mark(+1)
    val result = value
    meter.mark(-1)
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
    meter.mark(+1)
    future.whenComplete { (_, _) =>
      meter.mark(-1)
      ()
    }
  }

  def timedAndTrackedCompletionStage[T](
      timer: Timer,
      meter: Meter,
      future: => CompletionStage[T]): CompletionStage[T] = {
    Timed.completionStage(timer, trackedCompletionStage(meter, future))
  }

  def future[T](timer: Timer, future: => Future[T]): Future[T] = {
    val ctx = timer.time()
    val result = future
    result.onComplete(_ => ctx.stop())(DirectExecutionContext)
    result
  }

  def future[EC, T](timer: Timer, future: => concurrent.Future[EC, T]): concurrent.Future[EC, T] = {
    val ctx = timer.time()
    val result = future
    result.onComplete(_ => ctx.stop())(DirectExecutionContext)
    result
  }

  def trackedFuture[T](counter: Counter, future: => Future[T]): Future[T] = {
    counter.inc()
    future.andThen { case _ => counter.dec() }(DirectExecutionContext)
  }

  def trackedFuture[T](meter: Meter, future: => Future[T]): Future[T] = {
    meter.mark(+1)
    future.andThen { case _ => meter.mark(-1) }(DirectExecutionContext)
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
      .mapMaterializedValue {
        case (mat, done) =>
          done.onComplete(_ => ctx.stop())(DirectExecutionContext)
          mat
      }
  }

}
