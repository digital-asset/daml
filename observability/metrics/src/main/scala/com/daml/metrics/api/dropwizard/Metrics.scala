// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.dropwizard

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.codahale.metrics.Timer.Context
import com.codahale.{metrics => codahale}
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.{Gauges, MetricHandle, MetricsContext}

case class DropwizardTimer(name: String, metric: codahale.Timer) extends Timer {

  def update(duration: Long, unit: TimeUnit)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Unit = metric.update(duration, unit)

  def update(duration: Duration)(implicit
      context: MetricsContext
  ): Unit = metric.update(duration)
  override def time[T](call: => T)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): T = metric.time(() => call)
  override def startAsync()(implicit
      context: MetricsContext = MetricsContext.Empty
  ): TimerHandle = {
    val ctx = metric.time()
    DropwizardTimerHandle(ctx)
  }
}

final case class DropwizardTimerHandle(ctx: Context) extends TimerHandle {

  override def stop()(implicit context: MetricsContext): Unit = ctx.close()

}

sealed case class DropwizardMeter(name: String, metric: codahale.Meter) extends Meter {

  def mark(value: Long)(implicit
      context: MetricsContext
  ): Unit = metric.mark(value)

}

sealed case class DropwizardCounter(name: String, metric: codahale.Counter) extends Counter {

  override def inc()(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Unit = metric.inc

  override def inc(n: Long)(implicit
      context: MetricsContext
  ): Unit = metric.inc(n)

  override def dec()(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Unit = metric.dec

  override def dec(n: Long)(implicit
      context: MetricsContext
  ): Unit = metric.dec(n)

}

sealed case class DropwizardGauge[T](name: String, metric: Gauges.VarGauge[T], cleanUp: () => Unit)
    extends Gauge[T] {
  def updateValue(newValue: T): Unit = metric.updateValue(newValue)
  override def getValue: T = metric.getValue

  override def updateValue(f: T => T): Unit = metric.updateValue(f)

  override def close(): Unit = cleanUp()
}

sealed case class DropwizardHistogram(name: String, metric: codahale.Histogram)
    extends MetricHandle
    with Histogram {
  override def update(value: Long)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Unit = metric.update(value)
  override def update(value: Int)(implicit
      context: MetricsContext
  ): Unit = metric.update(value)
}

sealed case class AsyncGauge[T](valueSupplier: () => T) extends codahale.Gauge[T] {

  override def getValue: T = valueSupplier()
}
