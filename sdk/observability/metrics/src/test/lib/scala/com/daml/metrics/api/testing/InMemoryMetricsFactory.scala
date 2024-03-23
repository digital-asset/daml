// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.testing

import java.time.Duration
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import com.daml.metrics.api.MetricHandle.Gauge.{CloseableGauge, SimpleCloseableGauge}
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
import scala.jdk.CollectionConverters.CollectionHasAsScala

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
  ): MetricHandle.Timer = metrics.addTimer(name, context, InMemoryTimer(name, context))

  override def gauge[T](name: MetricName, initial: T, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Gauge[T] = metrics.addGauge(name, context, InMemoryGauge(name, context, initial))

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => T,
      description: String,
  )(implicit context: MetricsContext): CloseableGauge = {
    metrics.addAsyncGauge(name, context, gaugeSupplier)
    SimpleCloseableGauge(
      name,
      () => discard { metrics.asyncGauges.get(name).foreach(_.remove(context)) },
    )
  }

  override def meter(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Meter = metrics.addMeter(name, context, InMemoryMeter(name, context))

  override def counter(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Counter = metrics.addCounter(name, context, InMemoryCounter(name, context))

  override def histogram(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Histogram = metrics.addHistogram(name, context, InMemoryHistogram(name, context))

}

object InMemoryMetricsFactory extends InMemoryMetricsFactory {

  type StoredMetric[Metric] = ConcurrentMap[MetricsContext, Metric]
  type MetricsByName[Metric] = ConcurrentMap[MetricName, StoredMetric[Metric]]
  case class MetricsState(
      timers: MetricsByName[InMemoryTimer],
      gauges: MetricsByName[InMemoryGauge[_]],
      meters: MetricsByName[InMemoryMeter],
      counters: MetricsByName[InMemoryCounter],
      histograms: MetricsByName[InMemoryHistogram],
      asyncGauges: MetricsByName[() => Any],
  ) {

    def addTimer(name: MetricName, context: MetricsContext, timer: InMemoryTimer): Timer = {
      addOneToState(name, context, timer, timers)
      timer
    }

    def addGauge[T](
        name: MetricName,
        context: MetricsContext,
        gauge: InMemoryGauge[T],
    ): Gauge[T] = {
      addOneToState(name, context, gauge, gauges)
      gauge
    }

    def addAsyncGauge(
        name: MetricName,
        context: MetricsContext,
        gauge: () => Any,
    ): () => Any = {
      addOneToState(name, context, gauge, asyncGauges)
      gauge
    }

    def addMeter(name: MetricName, context: MetricsContext, meter: InMemoryMeter): InMemoryMeter = {
      addOneToState(name, context, meter, meters)
      meter
    }

    def addCounter(
        name: MetricName,
        context: MetricsContext,
        counter: InMemoryCounter,
    ): InMemoryCounter = {
      addOneToState(name, context, counter, counters)
      counter
    }

    def addHistogram(
        name: MetricName,
        context: MetricsContext,
        histogram: InMemoryHistogram,
    ): InMemoryHistogram = {
      addOneToState(name, context, histogram, histograms)
      histogram
    }

    private def addOneToState[T](
        name: MetricName,
        context: MetricsContext,
        metric: T,
        state: MetricsByName[T],
    ): Unit = {
      discard(state.getOrElseUpdate(name, TrieMap.empty).addOne(context -> metric))
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

  case class InMemoryTimer(override val name: String, initialContext: MetricsContext)
      extends Timer {
    val data: InMemoryHistogram = InMemoryHistogram(name, initialContext)

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

  case class InMemoryGauge[T](override val name: String, context: MetricsContext, initial: T)
      extends Gauge[T] {
    val value = new AtomicReference[T](initial)
    val closed = new AtomicBoolean(false)

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
      if (closed.get()) throw new IllegalStateException("Already closed")
  }

  case class InMemoryMeter(override val name: String, initialContext: MetricsContext)
      extends Meter {

    val markers: collection.concurrent.Map[MetricsContext, AtomicLong] = TrieMap()

    override def mark(value: Long)(implicit
        context: MetricsContext
    ): Unit = addToContext(markers, initialContext.merge(context), value)
  }

  case class InMemoryCounter(override val name: String, initialContext: MetricsContext)
      extends Counter {

    val markers: collection.concurrent.Map[MetricsContext, AtomicLong] = TrieMap()

    override def inc(value: Long)(implicit context: MetricsContext): Unit =
      addToContext(markers, initialContext.merge(context), value)

    override def dec(value: Long)(implicit context: MetricsContext): Unit =
      addToContext(markers, initialContext.merge(context), -value)

  }

  case class InMemoryHistogram(override val name: String, initialContext: MetricsContext)
      extends Histogram {

    private val internalRecordedValues
        : collection.concurrent.Map[MetricsContext, ConcurrentLinkedQueue[Long]] =
      TrieMap()

    def recordedValues: Map[MetricsContext, Seq[Long]] =
      internalRecordedValues.toMap.view.mapValues(_.asScala.toSeq).toMap

    override def update(value: Long)(implicit
        context: MetricsContext
    ): Unit = discard {
      internalRecordedValues.updateWith(initialContext.merge(context)) {
        case None =>
          val queue = new ConcurrentLinkedQueue[Long]()
          discard(queue.add(value))
          Some(queue)
        case Some(existingValues) =>
          discard(existingValues.add(value))
          Some(
            existingValues
          )
      }
    }

    override def update(value: Int)(implicit
        context: MetricsContext
    ): Unit = update(value.toLong)
  }
}
