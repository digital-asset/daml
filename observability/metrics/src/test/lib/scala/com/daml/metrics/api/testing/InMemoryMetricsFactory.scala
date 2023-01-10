// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.testing

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.api.MetricHandle.{
  Counter,
  Gauge,
  Histogram,
  LabeledMetricsFactory,
  Meter,
  Timer,
}
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{
  InMemoryCounter,
  InMemoryGauge,
  InMemoryHistogram,
  InMemoryMeter,
  InMemoryTimer,
  MetricsState,
}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import com.daml.scalautil.Statement.discard

import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}

class InMemoryMetricsFactory extends LabeledMetricsFactory {

  val metrics: MetricsState =
    MetricsState(
      timers = TrieMap.empty,
      gauges = TrieMap.empty,
      meters = TrieMap.empty,
      counters = TrieMap.empty,
      histograms = TrieMap.empty,
      asyncGauges = TrieMap.empty,
    )

  override def timer(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Timer = metrics.addTimer(name, context, InMemoryTimer(context))

  override def gauge[T](name: MetricName, initial: T, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Gauge[T] = metrics.addGauge(name, context, InMemoryGauge(context, initial))

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => T,
      description: String,
  )(implicit context: MetricsContext): CloseableGauge = {
    metrics.asyncGauges.addOne(name -> context -> gaugeSupplier)
    () => discard { metrics.asyncGauges.remove(name -> context) }
  }

  override def meter(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Meter = metrics.addMeter(name, context, InMemoryMeter(context))

  override def counter(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Counter = metrics.addCounter(name, context, InMemoryCounter(context))

  override def histogram(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Histogram = metrics.addHistogram(name, context, InMemoryHistogram(context))

}

object InMemoryMetricsFactory extends InMemoryMetricsFactory {

  type MetricIdentifier = (MetricName, MetricsContext)
  case class MetricsState(
      timers: ConcurrentMap[MetricIdentifier, InMemoryTimer],
      gauges: ConcurrentMap[MetricIdentifier, InMemoryGauge[_]],
      meters: ConcurrentMap[MetricIdentifier, InMemoryMeter],
      counters: ConcurrentMap[MetricIdentifier, InMemoryCounter],
      histograms: ConcurrentMap[MetricIdentifier, InMemoryHistogram],
      asyncGauges: ConcurrentMap[MetricIdentifier, () => Any],
  ) {

    def addTimer(name: MetricName, context: MetricsContext, timer: InMemoryTimer): Timer = {
      timers.addOne(name -> context -> timer)
      timer
    }

    def addGauge[T](
        name: MetricName,
        context: MetricsContext,
        gauge: InMemoryGauge[T],
    ): Gauge[T] = {
      gauges.addOne(name -> context -> gauge)
      gauge
    }

    def addMeter(name: MetricName, context: MetricsContext, meter: InMemoryMeter): InMemoryMeter = {
      meters.addOne(name -> context -> meter)
      meter
    }

    def addCounter(
        name: MetricName,
        context: MetricsContext,
        counter: InMemoryCounter,
    ): InMemoryCounter = {
      counters.addOne(name -> context -> counter)
      counter
    }

    def addHistogram(
        name: MetricName,
        context: MetricsContext,
        histogram: InMemoryHistogram,
    ): InMemoryHistogram = {
      histograms.addOne(name -> context -> histogram)
      histogram
    }
  }

  private def addToContext(
      markers: ConcurrentMap[MetricsContext, AtomicLong],
      context: MetricsContext,
      value: Long,
  ): Unit = discard {
    markers.updateWith(context) {
      case None => Some(new AtomicLong(value))
      case existingValue @ Some(existing) =>
        existing.addAndGet(value)
        existingValue
    }
  }

  case class InMemoryTimer(initialContext: MetricsContext) extends Timer {
    val data: InMemoryHistogram = InMemoryHistogram(initialContext)

    override def name: String = "test"

    override def update(duration: Long, unit: TimeUnit)(implicit context: MetricsContext): Unit =
      data.update(TimeUnit.MILLISECONDS.convert(duration, unit))

    override def update(duration: Duration)(implicit context: MetricsContext): Unit =
      data.update(TimeUnit.MILLISECONDS.convert(duration))

    override def time[T](call: => T)(implicit context: MetricsContext): T = {
      val start = System.nanoTime()
      val result = call
      val end = System.nanoTime()
      data.update(TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS))
      result
    }

    override def startAsync()(implicit startContext: MetricsContext): Timer.TimerHandle = {
      val start = System.nanoTime()
      new Timer.TimerHandle {
        override def stop()(implicit context: MetricsContext): Unit =
          data.update(
            TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
          )(startContext.merge(context))
      }
    }

  }

  case class InMemoryGauge[T](context: MetricsContext, initial: T) extends Gauge[T] {
    val value = new AtomicReference[T](initial)
    val closed = new AtomicBoolean(false)

    override def name: String = "test"

    override def updateValue(newValue: T): Unit = {
      checkClosed()
      value.set(newValue)
    }

    override def updateValue(f: T => T): Unit = {
      checkClosed()
      discard(value.updateAndGet(value => f(value)))
    }

    override def getValue: T = {
      checkClosed()
      value.get()
    }

    override def close(): Unit = closed.set(true)

    private def checkClosed(): Unit =
      if (closed.get()) throw new IllegalArgumentException("Already closed")
  }

  case class InMemoryMeter(initialContext: MetricsContext) extends Meter {

    val markers: collection.concurrent.Map[MetricsContext, AtomicLong] = TrieMap()

    override def name: String = "test"

    override def mark(value: Long)(implicit
        context: MetricsContext
    ): Unit = addToContext(markers, initialContext.merge(context), value)
  }

  case class InMemoryCounter(initialContext: MetricsContext) extends Counter {

    val markers: collection.concurrent.Map[MetricsContext, AtomicLong] = TrieMap()

    override def name: String = "test"

    override def inc(value: Long)(implicit context: MetricsContext): Unit =
      addToContext(markers, initialContext.merge(context), value)

    override def dec(value: Long)(implicit context: MetricsContext): Unit =
      addToContext(markers, initialContext.merge(context), -value)

  }

  case class InMemoryHistogram(initialContext: MetricsContext) extends Histogram {

    val values: collection.concurrent.Map[MetricsContext, Seq[Long]] = TrieMap()

    override def name: String = "test"

    override def update(value: Long)(implicit
        context: MetricsContext
    ): Unit = discard {
      values.updateWith(initialContext.merge(context)) {
        case None => Some(Seq(value))
        case Some(existingValues) =>
          Some(
            existingValues :+ value
          )
      }
    }

    override def update(value: Int)(implicit
        context: MetricsContext
    ): Unit = update(value.toLong)
  }
}
