// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.testing

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.daml.metrics.api.MetricHandle.{Counter, Factory, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.testing.TestingMetrics.{
  TestingInMemoryCounter,
  TestingInMemoryGauge,
  TestingInMemoryHistogram,
  TestingInMemoryMeter,
  TestingInMemoryTimer,
}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import com.daml.scalautil.Statement.discard

import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}

trait TestingInMemoryMetricsFactory extends Factory {

  override def timer(name: MetricName)(implicit
      context: MetricsContext
  ): MetricHandle.Timer = TestingInMemoryTimer(context)

  override def gauge[T](name: MetricName, initial: T)(implicit
      context: MetricsContext
  ): MetricHandle.Gauge[T] = TestingInMemoryGauge(context)

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => T,
  )(implicit context: MetricsContext): Unit = ()

  override def meter(name: MetricName)(implicit
      context: MetricsContext
  ): MetricHandle.Meter = TestingInMemoryMeter(context)

  override def counter(name: MetricName)(implicit
      context: MetricsContext
  ): MetricHandle.Counter = TestingInMemoryCounter(context)

  override def histogram(name: MetricName)(implicit
      context: MetricsContext
  ): MetricHandle.Histogram = TestingInMemoryHistogram(context)

}

object TestingInMemoryMetricsFactory extends TestingInMemoryMetricsFactory

object TestingMetrics {

  case class TestingInMemoryTimer(initialContext: MetricsContext) extends Timer {
    val runTimers: collection.concurrent.Map[MetricsContext, AtomicInteger] = TrieMap()

    override def name: String = "test"

    override def update(duration: Long, unit: TimeUnit)(implicit context: MetricsContext): Unit =
      incrementForContext(context)

    override def update(duration: Duration)(implicit context: MetricsContext): Unit =
      incrementForContext(context)

    override def time[T](call: => T)(implicit context: MetricsContext): T = {
      incrementForContext(context)
      call
    }
    override def startAsync()(implicit startContext: MetricsContext): Timer.TimerHandle =
      new Timer.TimerHandle {
        override def stop()(implicit context: MetricsContext): Unit =
          incrementForContext(startContext.merge(context))
      }

    private def incrementForContext(context: MetricsContext): Unit =
      discard {
        runTimers.updateWith(initialContext.merge(context)) {
          case None => Some(new AtomicInteger(1))
          case data @ Some(existing) =>
            existing.incrementAndGet()
            data
        }
      }

  }

  case class TestingInMemoryGauge[T](context: MetricsContext) extends Gauge[T] {
    val value = new AtomicReference[T]()

    override def name: String = "test"

    override def updateValue(newValue: T): Unit =
      value.set(newValue)

    override def getValue: T = value.get()
  }

  case class TestingInMemoryMeter(initialContext: MetricsContext) extends Meter {

    val markers: collection.concurrent.Map[MetricsContext, AtomicLong] = TrieMap()

    override def name: String = "test"

    override def mark(value: Long)(implicit
        context: MetricsContext
    ): Unit = addToContext(markers, initialContext.merge(context), value)
  }

  case class TestingInMemoryCounter(initialContext: MetricsContext) extends Counter {

    val markers: collection.concurrent.Map[MetricsContext, AtomicLong] = TrieMap()

    override def name: String = "test"

    override def inc(value: Long)(implicit context: MetricsContext): Unit =
      addToContext(markers, initialContext.merge(context), value)

    override def dec(value: Long)(implicit context: MetricsContext): Unit =
      addToContext(markers, initialContext.merge(context), -value)

    override def getCount: Long = 0

  }

  case class TestingInMemoryHistogram(initialContext: MetricsContext) extends Histogram {

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
}
