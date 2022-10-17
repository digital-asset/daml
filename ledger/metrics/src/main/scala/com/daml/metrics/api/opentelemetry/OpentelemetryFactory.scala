// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.opentelemetry

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.Gauges.VarGauge
import com.daml.metrics.api.MetricHandle.Timer.TimerStop
import com.daml.metrics.api.MetricHandle.{Counter, Factory, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.{MetricHandle, MetricName}
import io.opentelemetry.api.metrics.{
  LongCounter,
  LongHistogram,
  LongUpDownCounter,
  Meter => OtelMeter,
}

trait OpentelemetryFactory extends Factory {

  def otelMeter: OtelMeter
  override def timer(
      name: MetricName
  ): MetricHandle.Timer =
    OpentelemetryTimer(name, otelMeter.histogramBuilder(name).ofLongs().setUnit("ms").build())
  override def gauge[T](name: MetricName, initial: T): MetricHandle.Gauge[T] = {
    initial match {
      case longInitial: Long =>
        val varGauge = new VarGauge[Long](longInitial)
        otelMeter.gaugeBuilder(name).ofLongs().buildWithCallback(_.record(varGauge.getValue))
        OpentelemetryGauge(name, varGauge.asInstanceOf[VarGauge[T]])
      case doubleInitial: Double =>
        val varGauge = new VarGauge[Double](doubleInitial)
        otelMeter.gaugeBuilder(name).buildWithCallback(_.record(varGauge.getValue))
        OpentelemetryGauge(name, varGauge.asInstanceOf[VarGauge[T]])
      case _ =>
        // A NoOp guage as opentelemetry only supports longs and doubles
        OpentelemetryGauge(name, VarGauge(initial))
    }
  }
  override def gaugeWithSupplier[T](name: MetricName, gaugeSupplier: () => () => T): Unit = {
    val valueSupplier = gaugeSupplier()
    valueSupplier() match {
      case _: Long =>
        otelMeter
          .gaugeBuilder(name)
          .ofLongs()
          .buildWithCallback(_.record(valueSupplier().asInstanceOf[Long]))
        ()
      case _: Double =>
        otelMeter
          .gaugeBuilder(name)
          .buildWithCallback(_.record(valueSupplier().asInstanceOf[Double]))
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
  override def update(duration: Long, unit: TimeUnit): Unit =
    histogram.record(TimeUnit.MILLISECONDS.convert(duration, unit))
  override def time[T](call: => T): T = {
    val start = System.nanoTime()
    val result = call
    histogram.record(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS))
    result
  }
  override def startAsync(): TimerStop = {
    val start = System.nanoTime()
    () =>
      histogram.record(
        TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
      )
  }
  override def update(duration: Duration): Unit = update(duration.toNanos, TimeUnit.NANOSECONDS)
}

case class OpentelemetryGauge[T](name: String, varGauge: VarGauge[T]) extends Gauge[T] {
  override def updateValue(newValue: T): Unit = varGauge.updateValue(newValue)
  override def getValue: T = varGauge.getValue
}

case class OpentelemetryMeter(name: String, counter: LongCounter) extends Meter {
  override def mark(value: Long): Unit = counter.add(value)
}

case class OpentelemetryCounter(name: String, counter: LongUpDownCounter) extends Counter {
  override def inc(): Unit = counter.add(1)
  override def inc(n: Long): Unit = counter.add(n)
  override def dec(): Unit = counter.add(-1)
  override def dec(n: Long): Unit = counter.add(-n)
  override def getCount: Long = 0 // Not supported OpenTelemetry
}

case class OpentelemetryHistogram(name: String, histogram: LongHistogram) extends Histogram {
  override def update(value: Long): Unit = histogram.record(value)
  override def update(value: Int): Unit = histogram.record(value.toLong)
}
