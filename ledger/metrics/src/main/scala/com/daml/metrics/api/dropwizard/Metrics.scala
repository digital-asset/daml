// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.dropwizard

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.codahale.{metrics => codahale}
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.{Gauges, MetricHandle}

case class DropwizardTimer(name: String, metric: codahale.Timer) extends Timer {

  def update(duration: Long, unit: TimeUnit): Unit = metric.update(duration, unit)

  def update(duration: Duration): Unit = metric.update(duration)
  override def time[T](call: => T): T = metric.time(() => call)
  override def startAsync(): TimerHandle = {
    val ctx = metric.time()
    () => {
      ctx.stop()
      ()
    }
  }
}

sealed case class DropwizardMeter(name: String, metric: codahale.Meter) extends Meter {

  def mark(value: Long): Unit = metric.mark(value)

}

sealed case class DropwizardCounter(name: String, metric: codahale.Counter) extends Counter {

  override def inc(): Unit = metric.inc
  override def inc(n: Long): Unit = metric.inc(n)
  override def dec(): Unit = metric.dec
  override def dec(n: Long): Unit = metric.dec(n)

  override def getCount: Long = metric.getCount
}

sealed case class DropwizardGauge[T](name: String, metric: Gauges.VarGauge[T]) extends Gauge[T] {
  def updateValue(newValue: T): Unit = metric.updateValue(newValue)
  override def getValue: T = metric.getValue
}

sealed case class DropwizardHistogram(name: String, metric: codahale.Histogram)
    extends MetricHandle
    with Histogram {
  override def update(value: Long): Unit = metric.update(value)
  override def update(value: Int): Unit = metric.update(value)
}
