// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.opentelemetry

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.Gauges.VarGauge
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Counter, Factory, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import io.opentelemetry.api.metrics.{
  LongCounter,
  LongHistogram,
  LongUpDownCounter,
  Meter => OtelMeter,
}

trait OpenTelemetryFactory extends Factory {

  def otelMeter: OtelMeter
  override def timer(
      name: MetricName
  ): MetricHandle.Timer =
    OpentelemetryTimer(name, otelMeter.histogramBuilder(name).ofLongs().setUnit("ms").build())
  override def gauge[T](name: MetricName, initial: T)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): MetricHandle.Gauge[T] = {
    initial match {
      case longInitial: Int =>
        val varGauge = new VarGauge[Int](longInitial)
        otelMeter.gaugeBuilder(name).ofLongs().buildWithCallback { consumer =>
          val (value, context) = varGauge.getValueAndContext
          consumer.record(value.toLong, context.asAttributes)
        }
        OpentelemetryGauge(name, varGauge.asInstanceOf[VarGauge[T]])
      case longInitial: Long =>
        val varGauge = new VarGauge[Long](longInitial)
        otelMeter.gaugeBuilder(name).ofLongs().buildWithCallback { consumer =>
          val (value, context) = varGauge.getValueAndContext
          consumer.record(value, context.asAttributes)
        }
        OpentelemetryGauge(name, varGauge.asInstanceOf[VarGauge[T]])
      case doubleInitial: Double =>
        val varGauge = new VarGauge[Double](doubleInitial)
        otelMeter.gaugeBuilder(name).buildWithCallback { consumer =>
          val (value, context) = varGauge.getValueAndContext
          consumer.record(value, context.asAttributes)
        }
        OpentelemetryGauge(name, varGauge.asInstanceOf[VarGauge[T]])
      case _ =>
        // A NoOp guage as opentelemetry only supports longs and doubles
        OpentelemetryGauge(name, VarGauge(initial))
    }
  }

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => () => (T, MetricsContext),
  ): Unit = {
    val valueSupplier = gaugeSupplier()
    valueSupplier()._1 match {
      case _: Int =>
        otelMeter
          .gaugeBuilder(name)
          .ofLongs()
          .buildWithCallback { consumer =>
            val (value, context) = valueSupplier()
            consumer.record(value.asInstanceOf[Int].toLong, context.asAttributes)
          }
        ()
      case _: Long =>
        otelMeter
          .gaugeBuilder(name)
          .ofLongs()
          .buildWithCallback { consumer =>
            val (value, context) = valueSupplier()
            consumer.record(value.asInstanceOf[Long], context.asAttributes)
          }
        ()
      case _: Double =>
        otelMeter
          .gaugeBuilder(name)
          .buildWithCallback { consumer =>
            val (value, context) = valueSupplier()
            consumer.record(value.asInstanceOf[Double], context.asAttributes)
          }
        ()
      case _ =>
      // NoOp as opentelemetry only supports longs and doubles
    }
  }
  override def meter(name: MetricName): Meter = OpentelemetryMeter(
    name,
    otelMeter.counterBuilder(name).build(),
  )
  override def counter(name: MetricName): MetricHandle.Counter = OpentelemetryCounter(
    name,
    otelMeter.upDownCounterBuilder(name).build(),
  )
  override def histogram(name: MetricName): MetricHandle.Histogram = OpentelemetryHistogram(
    name,
    otelMeter.histogramBuilder(name).ofLongs().build(),
  )
}

case class OpentelemetryTimer(name: String, histogram: LongHistogram) extends Timer {

  override def update(duration: Long, unit: TimeUnit)(implicit
      context: MetricsContext
  ): Unit =
    histogram.record(
      TimeUnit.MILLISECONDS.convert(duration, unit),
      context.asAttributes,
    )
  override def time[T](call: => T)(implicit
      context: MetricsContext
  ): T = {
    val start = System.nanoTime()
    val result = call
    histogram.record(
      TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS),
      context.asAttributes,
    )
    result
  }
  override def startAsync()(implicit
      context: MetricsContext
  ): TimerHandle = {
    val start = System.nanoTime()
    () =>
      histogram.record(
        TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS),
        context.asAttributes,
      )
  }
  override def update(duration: Duration)(implicit
      context: MetricsContext
  ): Unit = update(duration.toNanos, TimeUnit.NANOSECONDS)
}

case class OpentelemetryGauge[T](name: String, varGauge: VarGauge[T]) extends Gauge[T] {
  override def updateValue(newValue: T)(implicit
      context: MetricsContext
  ): Unit = varGauge.updateValue(newValue)
  override def getValue: T = varGauge.getValue
}

case class OpentelemetryMeter(name: String, counter: LongCounter) extends Meter {
  override def mark(value: Long)(implicit
      context: MetricsContext
  ): Unit = counter.add(value, context.asAttributes)
}

case class OpentelemetryCounter(name: String, counter: LongUpDownCounter) extends Counter {
  override def inc()(implicit
      context: MetricsContext
  ): Unit = counter.add(1, context.asAttributes)
  override def inc(n: Long)(implicit
      context: MetricsContext
  ): Unit = counter.add(n, context.asAttributes)
  override def dec()(implicit
      context: MetricsContext
  ): Unit = counter.add(-1, context.asAttributes)
  override def dec(n: Long)(implicit
      context: MetricsContext
  ): Unit = counter.add(-n, context.asAttributes)
  override def getCount: Long = 0 // Not supported by OpenTelemetry
}

case class OpentelemetryHistogram(name: String, histogram: LongHistogram) extends Histogram {
  override def update(value: Long)(implicit
      context: MetricsContext
  ): Unit = histogram.record(value, context.asAttributes)
  override def update(value: Int)(implicit
      context: MetricsContext
  ): Unit = histogram.record(value.toLong, context.asAttributes)
}
