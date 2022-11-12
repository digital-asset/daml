// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.MetricHandle.Timer.TimerHandle

import scala.concurrent.{ExecutionContext, Future}

trait MetricHandle {
  def name: String
  def metricType: String // type string used for documentation purposes
}

object MetricHandle {

  trait Factory {

    def timer(name: MetricName)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Timer

    def gauge[T](name: MetricName, initial: T)(implicit
        context: MetricsContext
    ): Gauge[T]

    def gaugeWithSupplier[T](
        name: MetricName,
        gaugeSupplier: () => T,
    )(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit

    def meter(name: MetricName)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Meter

    def counter(name: MetricName)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Counter

    def histogram(name: MetricName)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Histogram

  }

  trait Timer extends MetricHandle {

    def metricType: String = "Timer"

    def update(duration: Long, unit: TimeUnit)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit

    def update(duration: Duration)(implicit
        context: MetricsContext
    ): Unit

    def time[T](call: => T)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): T

    def startAsync()(implicit
        context: MetricsContext = MetricsContext.Empty
    ): TimerHandle

    def timeFuture[T](call: => Future[T])(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Future[T] = {
      val timer = startAsync()
      val result = call
      result.onComplete(_ => timer.stop())(ExecutionContext.parasitic)
      result
    }
  }

  object Timer {

    trait TimerHandle {

      def stop()(implicit
          context: MetricsContext = MetricsContext.Empty
      ): Unit

    }

  }

  trait Gauge[T] extends MetricHandle {
    def metricType: String = "Gauge"

    def updateValue(newValue: T): Unit

    def updateValue(f: T => T): Unit = updateValue(f(getValue))

    def getValue: T
  }

  trait Meter extends MetricHandle {
    def metricType: String = "Meter"

    def mark()(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit = mark(1)
    def mark(value: Long)(implicit
        context: MetricsContext
    ): Unit

  }

  trait Counter extends MetricHandle {

    override def metricType: String = "Counter"
    def inc()(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit = inc(1)
    def inc(n: Long)(implicit
        context: MetricsContext
    ): Unit
    def dec()(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit = dec(1)
    def dec(n: Long)(implicit
        context: MetricsContext
    ): Unit
    def getCount: Long
  }

  trait Histogram extends MetricHandle {

    def metricType: String = "Histogram"
    def update(value: Long)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit
    def update(value: Int)(implicit
        context: MetricsContext
    ): Unit

  }

  object Histogram {
    val Bytes: MetricName = MetricName("bytes")
  }

}
