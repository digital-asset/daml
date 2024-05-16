// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.opentelemetry

import com.daml.metrics.{MetricsFilter, MetricsFilterConfig}
import com.daml.metrics.api.MetricQualification

import java.time.Duration
import java.util.concurrent.TimeUnit
import com.daml.metrics.api.MetricHandle.Gauge.{CloseableGauge, SimpleCloseableGauge}
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{
  Counter,
  Gauge,
  Histogram,
  LabeledMetricsFactory,
  Meter,
  Timer,
}
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryTimer.{
  DurationSuffix,
  TimerUnit,
  TimerUnitAndSuffix,
  convertNanosecondsToSeconds,
}
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricName, MetricsContext}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.{
  DoubleHistogram,
  LongCounter,
  LongHistogram,
  LongUpDownCounter,
  Meter => OtelMeter,
}
import org.slf4j.Logger

import java.util.concurrent.atomic.AtomicReference

class QualificationFilteringMetricsFactory(
    parent: LabeledMetricsFactory,
    qualifications: Set[MetricQualification],
    filters: Seq[MetricsFilterConfig],
) extends LabeledMetricsFactory {

  private val filter = new MetricsFilter(filters)

  private def include(info: MetricInfo): Boolean =
    filter.includeMetric(info.name.toString) && qualifications.contains(info.qualification)

  override def timer(info: MetricInfo)(implicit context: MetricsContext): Timer = if (include(info))
    parent.timer(info)
  else
    NoOpMetricsFactory.timer(info)
  override def gauge[T](info: MetricInfo, initial: T)(implicit
      context: MetricsContext
  ): Gauge[T] =
    if (include(info))
      parent.gauge(info, initial)
    else
      NoOpMetricsFactory.gauge(info, initial)

  override def gaugeWithSupplier[T](info: MetricInfo, gaugeSupplier: () => T)(implicit
      context: MetricsContext
  ): CloseableGauge = if (include(info))
    parent.gaugeWithSupplier(info, gaugeSupplier)
  else
    NoOpMetricsFactory.gaugeWithSupplier(info, gaugeSupplier)
  override def meter(info: MetricInfo)(implicit context: MetricsContext): Meter = if (include(info))
    parent.meter(info)
  else
    NoOpMetricsFactory.meter(info)

  override def counter(info: MetricInfo)(implicit context: MetricsContext): Counter = if (
    include(info)
  )
    parent.counter(info)
  else
    NoOpMetricsFactory.counter(info)
  override def histogram(info: MetricInfo)(implicit context: MetricsContext): Histogram =
    if (include(info))
      parent.histogram(info)
    else
      NoOpMetricsFactory.histogram(info)
}

class OpenTelemetryMetricsFactory(
    otelMeter: OtelMeter,
    knownHistograms: Set[String],
    onlyLogMissingHistograms: Option[Logger],
    globalMetricsContext: MetricsContext = MetricsContext(),
) extends LabeledMetricsFactory {

  override def timer(info: MetricInfo)(implicit
      context: MetricsContext
  ): MetricHandle.Timer = {
    if (!knownHistograms.contains(info.name)) {
      val msg =
        s"Timer with name ${info.name} is not a known histogram. Please add the name of this timer to the list of known histograms."
      onlyLogMissingHistograms match {
        // TODO(#17917) switch to warn
        case Some(logger) => logger.info(msg)
        case None => throw new IllegalArgumentException(msg)
      }
    }
    val nameWithSuffix =
      if (info.name.endsWith(DurationSuffix)) info.name :+ TimerUnit
      else info.name :+ TimerUnitAndSuffix

    OpenTelemetryTimer(
      info,
      otelMeter
        .histogramBuilder(nameWithSuffix)
        .setUnit(TimerUnit)
        .setDescription(info.description)
        .build(),
      globalMetricsContext.merge(context),
    )
  }
  override def gauge[T](info: MetricInfo, initial: T)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): MetricHandle.Gauge[T] = {
    val attributes = globalMetricsContext.merge(context).asAttributes
    val gauge = OpenTelemetryGauge(info, initial)

    val registered = initial match {
      case _: Int =>
        otelMeter
          .gaugeBuilder(info.name)
          .ofLongs()
          .setDescription(info.description)
          .buildWithCallback { consumer =>
            consumer.record(gauge.getValue.asInstanceOf[Int].toLong, attributes)
          }
      case _: Long =>
        otelMeter
          .gaugeBuilder(info.name)
          .ofLongs()
          .setDescription(info.description)
          .buildWithCallback { consumer =>
            consumer.record(gauge.getValue.asInstanceOf[Long], attributes)
          }
      case _: Double =>
        otelMeter.gaugeBuilder(info.name).setDescription(info.description).buildWithCallback {
          consumer =>
            consumer.record(gauge.getValue.asInstanceOf[Double], attributes)
        }
      case _ =>
        throw new IllegalArgumentException("Gauges support only numeric values.")
    }
    gauge.reference.set(Some(registered))
    gauge
  }

  override def gaugeWithSupplier[T](
      info: MetricInfo,
      valueSupplier: () => T,
  )(implicit
      context: MetricsContext = MetricsContext.Empty
  ): CloseableGauge = {
    val value = valueSupplier()
    val attributes = globalMetricsContext.merge(context).asAttributes
    value match {
      case _: Int =>
        val gaugeHandle = otelMeter
          .gaugeBuilder(info.name)
          .ofLongs()
          .setDescription(info.description)
          .buildWithCallback { consumer =>
            val value = valueSupplier()
            consumer.record(value.asInstanceOf[Int].toLong, attributes)
          }
        SimpleCloseableGauge(info, gaugeHandle)
      case _: Long =>
        val gaugeHandle = otelMeter
          .gaugeBuilder(info.name)
          .ofLongs()
          .setDescription(info.description)
          .buildWithCallback { consumer =>
            val value = valueSupplier()
            consumer.record(value.asInstanceOf[Long], attributes)
          }
        SimpleCloseableGauge(info, gaugeHandle)
      case _: Double =>
        val gaugeHandle = otelMeter
          .gaugeBuilder(info.name)
          .setDescription(info.description)
          .buildWithCallback { consumer =>
            val value = valueSupplier()
            consumer.record(value.asInstanceOf[Double], attributes)
          }
        SimpleCloseableGauge(info, gaugeHandle)
      case _ =>
        throw new IllegalArgumentException("Gauges support only numeric values.")
    }
  }

  override def meter(info: MetricInfo)(implicit
      context: MetricsContext
  ): Meter = OpenTelemetryMeter(
    info,
    otelMeter.counterBuilder(info.name).setDescription(info.description).build(),
    globalMetricsContext.merge(context),
  )

  override def counter(info: MetricInfo)(implicit
      context: MetricsContext
  ): MetricHandle.Counter = OpenTelemetryCounter(
    info,
    otelMeter.upDownCounterBuilder(info.name).setDescription(info.description).build(),
    globalMetricsContext.merge(context),
  )

  override def histogram(info: MetricInfo)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): MetricHandle.Histogram = OpenTelemetryHistogram(
    info,
    otelMeter.histogramBuilder(info.name).ofLongs().setDescription(info.description).build(),
    globalMetricsContext.merge(context),
  )

}

case class OpenTelemetryTimer(
    override val info: MetricInfo,
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
    val result =
      try { call }
      finally {
        histogram.record(
          convertNanosecondsToSeconds(System.nanoTime() - start),
          AttributesHelper.multiContextAsAttributes(timerContext, context),
        )
      }
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
  private[opentelemetry] val DurationSuffix = "duration"

  val TimerUnitAndSuffix: MetricName = MetricName(DurationSuffix, TimerUnit)

  private val NanosecondsInASecond = 1_000_000_000

  private def convertNanosecondsToSeconds(nanoseconds: Long): Double = {
    nanoseconds.toDouble / NanosecondsInASecond
  }
}

case class OpenTelemetryGauge[T](override val info: MetricInfo, initial: T) extends Gauge[T] {

  private val ref = new AtomicReference[T](initial)
  private[opentelemetry] val reference = new AtomicReference[Option[AutoCloseable]](None)

  override def updateValue(newValue: T): Unit = ref.set(newValue)

  override def getValue: T = ref.get()

  override def updateValue(f: T => T): Unit = {
    val _ = ref.updateAndGet(f(_))
  }

  override def close(): Unit = reference.getAndSet(None).foreach(_.close())

}

case class OpenTelemetryMeter(
    override val info: MetricInfo,
    counter: LongCounter,
    meterContext: MetricsContext,
) extends Meter {

  override def mark(value: Long)(implicit
      context: MetricsContext
  ): Unit = counter.add(value, AttributesHelper.multiContextAsAttributes(meterContext, context))
}

case class OpenTelemetryCounter(
    override val info: MetricInfo,
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

}

case class OpenTelemetryHistogram(
    override val info: MetricInfo,
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
  private[opentelemetry] def multiContextAsAttributes(
      rootContext: MetricsContext,
      contexts: MetricsContext*
  ): Attributes = {
    val extraLabels = contexts.flatMap(_.labels)
    // Only create a new Attributes object if the non root contexts contains labels.
    // Creation of an Attributes object causes re-allocation of the labels array twice which can become
    // expensive given how often instrumentation methods get executed
    if (extraLabels.isEmpty) rootContext.asAttributes
    else
      (rootContext.labels.toList ++ extraLabels)
        .foldLeft(Attributes.builder()) { case (builder, (key, value)) =>
          builder.put(key, value)
          builder
        }
        .build()
  }
}
