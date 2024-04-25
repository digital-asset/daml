// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.noop

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.{MetricInfo, MetricsContext}

final case class NoOpTimer(info: MetricInfo) extends Timer {
  override def update(duration: Long, unit: TimeUnit)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Unit = ()
  override def update(duration: Duration)(implicit
      context: MetricsContext
  ): Unit = ()
  override def time[T](call: => T)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): T = call
  override def startAsync()(implicit
      context: MetricsContext = MetricsContext.Empty
  ): TimerHandle = NoOpTimerHandle
}

case object NoOpTimerHandle extends TimerHandle {
  override def stop()(implicit context: MetricsContext): Unit = ()
}

final case class NoOpGauge[T](info: MetricInfo, value: T) extends Gauge[T] {

  private val ref = new AtomicReference[T](value)

  override def updateValue(newValue: T): Unit = ref.set(newValue)

  override def getValue: T = ref.get()

  override def updateValue(f: T => T): Unit = {
    val _ = ref.updateAndGet(f(_))
  }

  override def close(): Unit = ()
}

final case class NoOpMeter(info: MetricInfo) extends Meter {
  override def mark(value: Long)(implicit
      context: MetricsContext
  ): Unit = ()
}

final case class NoOpCounter(info: MetricInfo) extends Counter {

  override def inc(n: Long)(implicit context: MetricsContext): Unit =
    ()

  override def dec(n: Long)(implicit context: _root_.com.daml.metrics.api.MetricsContext): Unit =
    ()

}

final case class NoOpHistogram(info: MetricInfo) extends Histogram {

  override def update(value: Long)(implicit
      context: MetricsContext
  ): Unit = ()

  override def update(value: Int)(implicit
      context: _root_.com.daml.metrics.api.MetricsContext
  ): Unit = ()
}
