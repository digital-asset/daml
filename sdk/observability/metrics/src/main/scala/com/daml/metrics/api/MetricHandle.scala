// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import com.daml.metrics.api.MetricQualification

import java.time.Duration
import java.util.concurrent.TimeUnit
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/** Information about a metric used for documentation and online help
  *
  * @param name of the metric
  * @param description description exposed on prometheus and in the docs
  * @param qualification the qualification of the metric
  */
final case class MetricInfo(
    name: MetricName,
    summary: String,
    qualification: MetricQualification,
    description: String = "",
    labelsWithDescription: Map[String, String] = Map.empty,
) {
  def extend(extension: String): MetricInfo = copy(name = name :+ extension)
}

object MetricInfo {

  /** temporary implicit conversion to enable refactoring */
  implicit def fromName(name: MetricName): MetricInfo =
    MetricInfo(name, "", MetricQualification.Debug)
}

trait MetricHandle {
  def info: MetricInfo
  def metricType: String // type string used for documentation purposes

}

object MetricHandle {

  trait LabeledMetricsFactory {

    /** A timer represented as a histogram
      *  - For `OpenTelemetry` the timer is represented by a histogram.
      */
    def timer(info: MetricInfo)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Timer

    /** A gauge represents the current value being monitored, such as queue size, requests in flight, etc.
      * The values being monitored should be numeric for compatibility with multiple metric systems
      * (e.g. Prometheus).
      */
    def gauge[T](info: MetricInfo, initial: T)(implicit
        context: MetricsContext
    ): Gauge[T]

    // TODO(#17917) remove once migration to MetricInfo is completed
    def gauge[T](name: MetricName, initial: T, description: String)(implicit
        context: MetricsContext
    ): Gauge[T] = gauge(MetricInfo(name, "", MetricQualification.Debug, description), initial)

    /** Same as a gauge, but the value is read using the `gaugeSupplier` only when the metrics are observed.
      */
    def gaugeWithSupplier[T](
        info: MetricInfo,
        gaugeSupplier: () => T,
    )(implicit
        context: MetricsContext
    ): CloseableGauge

    // TODO(#17917) remove once migration to MetricInfo is completed
    def gaugeWithSupplier[T](
        name: MetricName,
        gaugeSupplier: () => T,
        description: String,
    )(implicit
        context: MetricsContext
    ): CloseableGauge =
      gaugeWithSupplier(MetricInfo(name, "", MetricQualification.Debug, description), gaugeSupplier)

    /** A meter represents a monotonically increasing value.
      * In Prometheus this is actually represented by a `Counter`.
      * Note that meters should never decrease as the data is then skewed and unusable!
      */
    def meter(info: MetricInfo)(implicit
        context: MetricsContext
    ): Meter

    // TODO(#17917) remove once migration to MetricInfo is completed
    def meter(name: MetricName, description: String)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Meter = meter(MetricInfo(name, "", MetricQualification.Debug, description))

    /** A counter represents a value that can go up and down.
      *  A counter is actually represented as a gauge.
      *  We can think of a counter as a gauge with a richer API.
      */
    def counter(info: MetricInfo)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Counter

    /** A histogram represents a `bucketized` view of the data.
      *  In most cases the boundaries of the buckets should be manually configured for the monitored data.
      */
    def histogram(info: MetricInfo)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Histogram

  }

  trait Timer extends MetricHandle {

    def metricType: String = "Timer"

    def update(duration: Long, unit: TimeUnit)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit

    def update(duration: Duration)(implicit
        context: MetricsContext
    ): Unit

    def time[T](call: => T)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): T

    def startAsync()(implicit
        context: MetricsContext = MetricsContext.Empty
    ): TimerHandle

    def timeFuture[T](call: => Future[T])(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Future[T] = {
      val timer = startAsync()
      val result = call
      result.onComplete(_ => timer.stop())(ExecutionContext.parasitic)
      result
    }

  }

  object Timer {

    trait TimerHandle {

      def stop()(implicit
          context: MetricsContext = MetricsContext.Empty
      ): Unit

    }

  }

  trait Gauge[T] extends MetricHandle with CloseableGauge {
    def metricType: String = "Gauge"

    def updateValue(newValue: T): Unit

    def updateValue(f: T => T): Unit

    def getValue: T
  }

  object Gauge {

    /** Because gauges represent a specific value at a given time, there is a distinction between the value of a gauge no
      * longer being updated vs. the value no longer existing (contrary to how meters, histograms work). Because of this reasoning
      * gauges have to be closed after usage.
      */
    trait CloseableGauge extends AutoCloseable with MetricHandle

    case class SimpleCloseableGauge(override val info: MetricInfo, delegate: AutoCloseable)
        extends CloseableGauge {
      override def metricType: String = "Gauge"
      override def close(): Unit = delegate.close()
    }
  }

  trait Meter extends MetricHandle {
    def metricType: String = "Meter"

    def mark()(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit = mark(1)
    def mark(value: Long)(implicit
        context: MetricsContext
    ): Unit

  }

  trait Counter extends MetricHandle {

    override def metricType: String = "Counter"
    def inc()(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit = inc(1)
    def inc(n: Long)(implicit
        context: MetricsContext
    ): Unit
    def dec()(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit = dec(1)
    def dec(n: Long)(implicit
        context: MetricsContext
    ): Unit

  }

  trait Histogram extends MetricHandle {

    def metricType: String = "Histogram"
    def update(value: Long)(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Unit
    def update(value: Int)(implicit
        context: MetricsContext
    ): Unit

  }

  object Histogram {
    val Bytes: MetricName = MetricName("bytes")
  }

}
