// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.MetricHandle.Timer.TimerStop

import scala.concurrent.{ExecutionContext, Future}

trait MetricHandle {
  def name: String
  def metricType: String // type string used for documentation purposes
}

object MetricHandle {

  trait Factory {

    def prefix: MetricName

    def timer(name: MetricName): Timer

    def gauge[T](name: MetricName, initial: T): Gauge[T]

    def gaugeWithSupplier[T](
        name: MetricName,
        gaugeSupplier: () => () => T,
    ): Unit

    def meter(name: MetricName): Meter

    def counter(name: MetricName): Counter

    def histogram(name: MetricName): Histogram

  }

  trait Timer extends MetricHandle {

    def metricType: String = "Timer"

    def update(duration: Long, unit: TimeUnit): Unit

    def update(duration: Duration): Unit

    def time[T](call: => T): T

    def startAsync(): TimerStop

    def timeFuture[T](call: => Future[T]): Future[T] = {
      val stop = startAsync()
      val result = call
      result.onComplete(_ => stop())(ExecutionContext.parasitic)
      result
    }
  }

  object Timer {
    type TimerStop = () => Unit
  }

  trait Gauge[T] extends MetricHandle {
    def metricType: String = "Gauge"

    def updateValue(newValue: T): Unit

    def updateValue(f: T => T): Unit = updateValue(f(getValue))

    def getValue: T
  }

  trait Meter extends MetricHandle {
    def metricType: String = "Meter"

    def mark(): Unit = mark(1)
    def mark(value: Long): Unit

  }

  trait Counter extends MetricHandle {

    override def metricType: String = "Counter"
    def inc(): Unit
    def inc(n: Long): Unit
    def dec(): Unit
    def dec(n: Long): Unit
    def getCount: Long
  }

  trait Histogram extends MetricHandle {

    def metricType: String = "Histogram"
    def update(value: Long): Unit
    def update(value: Int): Unit

  }

}
