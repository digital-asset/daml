// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.opentelemetry

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.buildinfo.BuildInfo
import com.daml.metrics.api.Gauges.VarGauge
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Counter, Factory, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.opentelemetry.OpenTelemetryTimer.{
  TimerUnit,
  TimerUnitAndSuffix,
  convertNanosecondsToSeconds,
}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.{
  DoubleHistogram,
  LongCounter,
  LongHistogram,
  LongUpDownCounter,
  Meter => OtelMeter,
}

class OpenTelemetryFactory(otelMeter: OtelMeter) extends Factory {

  val globalMetricsContext: MetricsContext = MetricsContext(
    Map("daml_version" -> BuildInfo.Version)
  )

  override def timer(
      name: MetricName
  )(implicit
      context: MetricsContext = MetricsContext.Empty
  ): MetricHandle.Timer = {
    val nameWithSuffix = name :+ TimerUnitAndSuffix
    OpenTelemetryTimer(
      nameWithSuffix,
      otelMeter.histogramBuilder(nameWithSuffix).setUnit(TimerUnit).build(),
      globalMetricsContext.merge(context),
    )
  }
  override def gauge[T](name: MetricName, initial: T)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): MetricHandle.Gauge[T] = {
    val attributes = globalMetricsContext.merge(context).asAttributes
    initial match {
      case longInitial: Int =>
        val varGauge = new VarGauge[Int](longInitial)
        otelMeter.gaugeBuilder(name).ofLongs().buildWithCallback { consumer =>
          consumer.record(varGauge.getValue.toLong, attributes)
        }
        OpenTelemetryGauge(name, varGauge.asInstanceOf[VarGauge[T]])
      case longInitial: Long =>
        val varGauge = new VarGauge[Long](longInitial)
        otelMeter.gaugeBuilder(name).ofLongs().buildWithCallback { consumer =>
          consumer.record(varGauge.getValue, attributes)
        }
        OpenTelemetryGauge(name, varGauge.asInstanceOf[VarGauge[T]])
      case doubleInitial: Double =>
        val varGauge = new VarGauge[Double](doubleInitial)
        otelMeter.gaugeBuilder(name).buildWithCallback { consumer =>
          consumer.record(varGauge.getValue, attributes)
        }
        OpenTelemetryGauge(name, varGauge.asInstanceOf[VarGauge[T]])
      case _ =>
        // A NoOp gauge as OpenTelemetry only supports longs and doubles
        OpenTelemetryGauge(name, VarGauge(initial))
    }
  }

  override def gaugeWithSupplier[T](
      name: MetricName,
      valueSupplier: () => T,
  )(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Unit = {
    val value = valueSupplier()
    val attributes = globalMetricsContext.merge(context).asAttributes
    value match {
      case _: Int =>
        otelMeter
          .gaugeBuilder(name)
          .ofLongs()
          .buildWithCallback { consumer =>
            val value = valueSupplier()
            consumer.record(value.asInstanceOf[Int].toLong, attributes)
          }
        ()
      case _: Long =>
        otelMeter
          .gaugeBuilder(name)
          .ofLongs()
          .buildWithCallback { consumer =>
            val value = valueSupplier()
            consumer.record(value.asInstanceOf[Long], attributes)
          }
        ()
      case _: Double =>
        otelMeter
          .gaugeBuilder(name)
          .buildWithCallback { consumer =>
            val value = valueSupplier()
            consumer.record(value.asInstanceOf[Double], attributes)
          }
        ()
      case _ =>
      // NoOp as opentelemetry only supports longs and doubles
    }
  }

  override def meter(name: MetricName)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Meter = OpenTelemetryMeter(
    name,
    otelMeter.counterBuilder(name).build(),
    globalMetricsContext.merge(context),
  )

  override def counter(name: MetricName)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): MetricHandle.Counter = OpenTelemetryCounter(
    name,
    otelMeter.upDownCounterBuilder(name).build(),
    globalMetricsContext.merge(context),
  )

  override def histogram(name: MetricName)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): MetricHandle.Histogram = OpenTelemetryHistogram(
    name,
    otelMeter.histogramBuilder(name).ofLongs().build(),
    globalMetricsContext.merge(context),
  )

}

case class OpenTelemetryTimer(
    name: String,
    histogram: DoubleHistogram,
    timerContext: MetricsContext,
) extends Timer {

  override def update(duration: Long, unit: TimeUnit)(implicit
      context: MetricsContext
  ): Unit =
    histogram.record(
      convertNanosecondsToSeconds(TimeUnit.NANOSECONDS.convert(duration, unit)),
      AttributesHelper.multiContextAsAttributes(timerContext, context),
    )
  override def time[T](call: => T)(implicit
      context: MetricsContext
  ): T = {
    val start = System.nanoTime()
    val result = call
    histogram.record(
      convertNanosecondsToSeconds(System.nanoTime() - start),
      AttributesHelper.multiContextAsAttributes(timerContext, context),
    )
    result
  }

  override def startAsync()(implicit startContext: MetricsContext): TimerHandle = {
    val start = System.nanoTime()
    new TimerHandle {
      override def stop()(implicit stopContext: MetricsContext): Unit = {
        histogram.record(
          convertNanosecondsToSeconds(System.nanoTime() - start),
          AttributesHelper.multiContextAsAttributes(timerContext, startContext, stopContext),
        )
      }
    }
  }

  override def update(duration: Duration)(implicit
      context: MetricsContext
  ): Unit = update(duration.toNanos, TimeUnit.NANOSECONDS)
}
object OpenTelemetryTimer {

  private[opentelemetry] val TimerUnit: String = "seconds"
  val TimerUnitAndSuffix: MetricName = MetricName("duration", TimerUnit)

  private val NanoSecondsInASecond = 1_000_000_000

  private def convertNanosecondsToSeconds[T](nanoseconds: Long): Double = {
    nanoseconds.toDouble / NanoSecondsInASecond
  }
}

case class OpenTelemetryGauge[T](name: String, varGauge: VarGauge[T]) extends Gauge[T] {

  override def updateValue(newValue: T): Unit = varGauge.updateValue(newValue)

  override def getValue: T = varGauge.getValue

}

case class OpenTelemetryMeter(name: String, counter: LongCounter, meterContext: MetricsContext)
    extends Meter {

  override def mark(value: Long)(implicit
      context: MetricsContext
  ): Unit = counter.add(value, AttributesHelper.multiContextAsAttributes(meterContext, context))
}

case class OpenTelemetryCounter(
    name: String,
    counter: LongUpDownCounter,
    counterContext: MetricsContext,
) extends Counter {

  override def inc()(implicit
      context: MetricsContext
  ): Unit = counter.add(1, AttributesHelper.multiContextAsAttributes(counterContext, context))

  override def inc(n: Long)(implicit
      context: MetricsContext
  ): Unit = counter.add(n, AttributesHelper.multiContextAsAttributes(counterContext, context))

  override def dec()(implicit
      context: MetricsContext
  ): Unit = counter.add(-1, AttributesHelper.multiContextAsAttributes(counterContext, context))

  override def dec(n: Long)(implicit
      context: MetricsContext
  ): Unit = counter.add(-n, AttributesHelper.multiContextAsAttributes(counterContext, context))
  override def getCount: Long = 0 // Not supported by OpenTelemetry

}

case class OpenTelemetryHistogram(
    name: String,
    histogram: LongHistogram,
    histogramContext: MetricsContext,
) extends Histogram {

  override def update(value: Long)(implicit
      context: MetricsContext
  ): Unit =
    histogram.record(value, AttributesHelper.multiContextAsAttributes(histogramContext, context))

  override def update(value: Int)(implicit
      context: MetricsContext
  ): Unit = histogram.record(
    value.toLong,
    AttributesHelper.multiContextAsAttributes(histogramContext, context),
  )
}

private object AttributesHelper {

  /** Merges multiple [[MetricsContext]] into a single [[Attributes]] object.
    * The labels from all the contexts are added as attributes.
    * If the same label key is defined in multiple contexts, the value from the last metric context will be used.
    */
  private[opentelemetry] def multiContextAsAttributes(context: MetricsContext*): Attributes = {
    context
      .foldLeft(Attributes.builder()) { (builder, context) =>
        context.labels.foreachEntry { (key, value) =>
          builder.put(key, value)
        }
        builder
      }
      .build()
  }
}
