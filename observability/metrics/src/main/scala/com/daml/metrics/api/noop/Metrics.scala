// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.noop

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.MetricsContext

case class NoOpTimer(name: String) extends Timer {
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

case class NoOpGauge[T](name: String, value: T) extends Gauge[T] {

  override def updateValue(newValue: T): Unit = ()

  override def updateValue(f: T => T): Unit = ()

  override def getValue: T = value

  override def close(): Unit = ()
}

case class NoOpMeter(name: String) extends Meter {
  override def mark(value: Long)(implicit
      context: MetricsContext
  ): Unit = ()
}

case class NoOpCounter(name: String) extends Counter {

  override def inc(n: Long)(implicit context: MetricsContext): Unit =
    ()

  override def dec(n: Long)(implicit context: _root_.com.daml.metrics.api.MetricsContext): Unit =
    ()

}

case class NoOpHistogram(name: String) extends Histogram {

  override def update(value: Long)(implicit
      context: MetricsContext
  ): Unit = ()

  override def update(value: Int)(implicit
      context: _root_.com.daml.metrics.api.MetricsContext
  ): Unit = ()
}
