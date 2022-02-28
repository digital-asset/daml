// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.scaladsl.{Keep, Source}
import com.daml.concurrent
import io.prometheus.client._

import scala.concurrent.{ExecutionContext, Future}

object TimedNative {

  def value[T](histogram: Histogram, value: => T): T =
    histogram.time(() => value)

  def trackedValue[T](gauge: Gauge, value: => T): T = {
    gauge.inc()
    val result = value
    gauge.dec()
    result
  }

  def timedAndTrackedValue[T](histogram: Histogram, gauge: Gauge, value: => T): T = {
    TimedNative.value(histogram, trackedValue(gauge, value))
  }

  def completionStage[T](histogram: Histogram, future: => CompletionStage[T]): CompletionStage[T] = {
    val timer = histogram.startTimer()
    future.whenComplete { (_, _) =>
      timer.observeDuration()
      ()
    }
  }

  def trackedCompletionStage[T](gauge: Gauge, future: => CompletionStage[T]): CompletionStage[T] = {
    gauge.inc()
    future.whenComplete { (_, _) =>
      gauge.dec()
      ()
    }
  }

  def timedAndTrackedCompletionStage[T](
      histogram: Histogram,
      gauge: Gauge,
      future: => CompletionStage[T],
  ): CompletionStage[T] = {
    TimedNative.completionStage(histogram, trackedCompletionStage(gauge, future))
  }

  def future[T](histogram: Histogram, future: => Future[T]): Future[T] = {
    val timer = histogram.startTimer()
    val result = future
    result.onComplete(_ => timer.observeDuration())(ExecutionContext.parasitic)
    result
  }

  def future[EC, T](histogram: Histogram, future: => concurrent.Future[EC, T]): concurrent.Future[EC, T] = {
    val timer = histogram.startTimer()
    val result = future
    result.onComplete(_ => timer.observeDuration())(concurrent.ExecutionContext.parasitic)
    result
  }

  def trackedFuture[T](gauge: Gauge, future: => Future[T]): Future[T] = {
    gauge.inc()
    future.andThen { case _ => gauge.dec() }(ExecutionContext.parasitic)
  }

  def timedAndTrackedFuture[T](histogram: Histogram, gauge: Gauge, future: => Future[T]): Future[T] =
    TimedNative.future(histogram, trackedFuture(gauge, future))

  def source[Out, Mat](histogram: Histogram, source: => Source[Out, Mat]): Source[Out, Mat] = {
    val timer = histogram.startTimer()
    source
      .watchTermination()(Keep.both[Mat, Future[Done]])
      .mapMaterializedValue { case (mat, done) =>
        done.onComplete(_ => timer.observeDuration())(ExecutionContext.parasitic)
        mat
      }
  }

}
