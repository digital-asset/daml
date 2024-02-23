// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.testing

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.MetricHandle.Gauge.{CloseableGauge, SimpleCloseableGauge}
import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, MetricsFactory, Timer}
import com.daml.metrics.api.testing.ProxyMetricsFactory.{
  ProxyCounter,
  ProxyGauge,
  ProxyHistogram,
  ProxyMeter,
  ProxyTimer,
}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}

import scala.annotation.nowarn

@nowarn("cat=deprecation")
class ProxyMetricsFactory(firstTarget: MetricsFactory, secondTarget: MetricsFactory)
    extends LabeledMetricsFactory {

  private val targets = Seq(firstTarget, secondTarget)

  override def timer(
      name: MetricName,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Timer = ProxyTimer(
    name,
    targets.map(_.timer(name, description)),
  )

  override def gauge[T](
      name: MetricName,
      initial: T,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Gauge[T] = ProxyGauge(
    name,
    targets.map(
      _.gauge(name, initial, description)
    ),
  )

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => T,
      description: String,
  )(implicit context: MetricsContext): CloseableGauge = {
    val closingTargets = targets.map(_.gaugeWithSupplier(name, gaugeSupplier, description))
    SimpleCloseableGauge(name, () => closingTargets.foreach(_.close()))
  }

  override def meter(
      name: MetricName,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Meter = ProxyMeter(
    name,
    targets.map(_.meter(name, description)),
  )

  override def counter(
      name: MetricName,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Counter = ProxyCounter(
    name,
    targets.map(_.counter(name, description)),
  )

  override def histogram(
      name: MetricName,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Histogram = ProxyHistogram(
    name,
    targets.map(_.histogram(name, description)),
  )
}

object ProxyMetricsFactory {

  case class ProxyTimer(override val name: String, targets: Seq[MetricHandle.Timer])
      extends MetricHandle.Timer {

    override def update(duration: Long, unit: TimeUnit)(implicit
        context: MetricsContext
    ): Unit = targets.foreach(_.update(duration, unit))

    override def update(duration: Duration)(implicit
        context: MetricsContext
    ): Unit = targets.foreach(_.update(duration))

    override def time[T](call: => T)(implicit
        context: MetricsContext
    ): T = {
      val runningTimer = startAsync()
      try { call }
      finally { runningTimer.stop() }
    }

    override def startAsync()(implicit
        context: MetricsContext
    ): MetricHandle.Timer.TimerHandle = {
      val startedTimers = targets.map(_.startAsync())
      new Timer.TimerHandle {
        override def stop()(implicit context: MetricsContext): Unit =
          startedTimers.foreach(_.stop())
      }
    }

  }

  case class ProxyGauge[T](override val name: String, targets: Seq[MetricHandle.Gauge[T]])
      extends MetricHandle.Gauge[T] {

    override def updateValue(newValue: T): Unit = targets.foreach(_.updateValue(newValue))
    override def updateValue(f: T => T): Unit = targets.foreach(_.updateValue(f))
    override def getValue: T = targets.head.getValue

    override def close(): Unit = {
      targets.foreach(_.close())
    }
  }

  case class ProxyMeter(override val name: String, targets: Seq[MetricHandle.Meter])
      extends MetricHandle.Meter {

    override def mark(value: Long)(implicit
        context: MetricsContext
    ): Unit = targets.foreach(_.mark(value))

  }

  case class ProxyCounter(override val name: String, targets: Seq[MetricHandle.Counter])
      extends MetricHandle.Counter {

    override def inc(n: Long)(implicit context: MetricsContext): Unit =
      targets.foreach(_.inc(n))

    override def dec(n: Long)(implicit context: MetricsContext): Unit =
      targets.foreach(_.dec(n))

  }

  case class ProxyHistogram(override val name: String, targets: Seq[MetricHandle.Histogram])
      extends MetricHandle.Histogram {

    override def update(value: Long)(implicit
        context: MetricsContext
    ): Unit = targets.foreach(_.update(value))

    override def update(value: Int)(implicit
        context: MetricsContext
    ): Unit = targets.foreach(_.update(value))
  }

}
