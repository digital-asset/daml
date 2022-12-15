// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.testing

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.daml.metrics.api.MetricHandle.{Counter, Factory, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{
  InMemoryCounter,
  InMemoryGauge,
  InMemoryHistogram,
  InMemoryMeter,
  InMemoryTimer,
}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import com.daml.scalautil.Statement.discard

import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}

class InMemoryMetricsFactory extends Factory {

  val asyncGauges: ConcurrentMap[(MetricName, MetricsContext), () => Any] =
    TrieMap[(MetricName, MetricsContext), () => Any]()

  override def timer(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Timer = InMemoryTimer(context)

  override def gauge[T](name: MetricName, initial: T, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Gauge[T] = InMemoryGauge(context)

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => T,
      description: String,
  )(implicit context: MetricsContext): Unit = asyncGauges.addOne(name -> context -> gaugeSupplier)

  override def meter(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Meter = InMemoryMeter(context)

  override def counter(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Counter = InMemoryCounter(context)

  override def histogram(name: MetricName, description: String)(implicit
      context: MetricsContext
  ): MetricHandle.Histogram = InMemoryHistogram(context)

}

object InMemoryMetricsFactory extends InMemoryMetricsFactory {

  private def addToContext[T](
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
    val data = InMemoryHistogram(initialContext)

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

  case class InMemoryGauge[T](context: MetricsContext) extends Gauge[T] {
    val value = new AtomicReference[T]()

    override def name: String = "test"

    override def updateValue(newValue: T): Unit =
      value.set(newValue)

    override def getValue: T = value.get()
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

    override def getCount: Long = 0

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
