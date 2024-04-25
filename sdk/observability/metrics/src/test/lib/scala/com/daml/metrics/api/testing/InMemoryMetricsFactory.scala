// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricName, MetricsContext}
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

  override def timer(info: MetricInfo)(implicit
      context: MetricsContext
  ): MetricHandle.Timer = metrics.addTimer(context, InMemoryTimer(info, context))

  override def gauge[T](info: MetricInfo, initial: T)(implicit
      context: MetricsContext
  ): MetricHandle.Gauge[T] = metrics.addGauge(context, InMemoryGauge(info, context, initial))

  override def gaugeWithSupplier[T](
      info: MetricInfo,
      gaugeSupplier: () => T,
  )(implicit context: MetricsContext): CloseableGauge = {
    metrics.addAsyncGauge(info.name, context, gaugeSupplier)
    SimpleCloseableGauge(
      info,
      () => discard { metrics.asyncGauges.get(info.name).foreach(_.remove(context)) },
    )
  }

  override def meter(info: MetricInfo)(implicit
      context: MetricsContext
  ): MetricHandle.Meter = metrics.addMeter(context, InMemoryMeter(info, context))

  override def counter(info: MetricInfo)(implicit
      context: MetricsContext
  ): MetricHandle.Counter = metrics.addCounter(context, InMemoryCounter(info, context))

  override def histogram(info: MetricInfo)(implicit
      context: MetricsContext
  ): MetricHandle.Histogram = metrics.addHistogram(context, InMemoryHistogram(info, context))

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

    def addTimer(context: MetricsContext, timer: InMemoryTimer): Timer = {
      addOneToState(timer.info.name, context, timer, timers)
      timer
    }

    def addGauge[T](
        context: MetricsContext,
        gauge: InMemoryGauge[T],
    ): Gauge[T] = {
      addOneToState(gauge.info.name, context, gauge, gauges)
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

    def addMeter(context: MetricsContext, meter: InMemoryMeter): InMemoryMeter = {
      addOneToState(meter.info.name, context, meter, meters)
      meter
    }

    def addCounter(
        context: MetricsContext,
        counter: InMemoryCounter,
    ): InMemoryCounter = {
      addOneToState(counter.info.name, context, counter, counters)
      counter
    }

    def addHistogram(
        context: MetricsContext,
        histogram: InMemoryHistogram,
    ): InMemoryHistogram = {
      addOneToState(histogram.info.name, context, histogram, histograms)
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

  final case class InMemoryTimer(override val info: MetricInfo, initialContext: MetricsContext)
      extends Timer {
    val data: InMemoryHistogram = InMemoryHistogram(info, initialContext)

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

  final case class InMemoryGauge[T](
      override val info: MetricInfo,
      context: MetricsContext,
      initial: T,
  ) extends Gauge[T] {
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

  final case class InMemoryMeter(override val info: MetricInfo, initialContext: MetricsContext)
      extends Meter {

    val markers: collection.concurrent.Map[MetricsContext, AtomicLong] = TrieMap()

    override def mark(value: Long)(implicit
        context: MetricsContext
    ): Unit = addToContext(markers, initialContext.merge(context), value)
  }

  case class InMemoryCounter(override val info: MetricInfo, initialContext: MetricsContext)
      extends Counter {

    val markers: collection.concurrent.Map[MetricsContext, AtomicLong] = TrieMap()

    override def inc(value: Long)(implicit context: MetricsContext): Unit =
      addToContext(markers, initialContext.merge(context), value)

    override def dec(value: Long)(implicit context: MetricsContext): Unit =
      addToContext(markers, initialContext.merge(context), -value)

  }

  final case class InMemoryHistogram(override val info: MetricInfo, initialContext: MetricsContext)
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
