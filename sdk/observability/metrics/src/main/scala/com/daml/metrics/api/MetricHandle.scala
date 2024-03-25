// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

trait MetricHandle {
  def name: String
  def metricType: String // type string used for documentation purposes
}

object MetricHandle {

  @deprecated("Replaced by LabeledMetricsFactory, prefer it's usage", since = "2.7.0")
  trait MetricsFactory {

    /** A timer can be represented by either a summary or a histogram.
      *  - For `Dropwizard` the timer is represented as a summary.
      *  - For `OpenTelemetry` the timer is represented by a histogram.
      */
    def timer(name: MetricName, description: String = "")(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Timer

    /** A gauge represents the current value being monitored, such as queue size, requests in flight, etc.
      * The values being monitored should be numeric for compatibility with multiple metric systems
      * (e.g. Prometheus).
      */
    def gauge[T](name: MetricName, initial: T, description: String = "")(implicit
        context: MetricsContext
    ): Gauge[T]

    /** Same as a gauge, but the value is read using the `gaugeSupplier` only when the metrics are observed.
      */
    def gaugeWithSupplier[T](
        name: MetricName,
        gaugeSupplier: () => T,
        description: String = "",
    )(implicit
        context: MetricsContext
    ): CloseableGauge

    /** A meter represents a monotonically increasing value.
      * In Prometheus this is actually represented by a `Counter`.
      * Note that meters should never decrease as the data is then skewed and unusable!
      */
    def meter(name: MetricName, description: String = "")(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Meter

    /** A counter represents a value that can go up and down.
      *  For both `Dropwizard` and `OpenTelemetry` a counter is actually represented as a gauge.
      *  We can think of a counter as a gauge with a richer API.
      */
    def counter(name: MetricName, description: String = "")(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Counter

    /** A histogram represents a `bucketized` view of the data.
      *  In most cases the boundaries of the buckets should be manually configured for the monitored data.
      */
    def histogram(name: MetricName, description: String = "")(implicit
        context: MetricsContext = MetricsContext.Empty
    ): Histogram

  }

  @nowarn("cat=deprecation")
  trait LabeledMetricsFactory extends MetricsFactory

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

    case class SimpleCloseableGauge(name: String, delegate: AutoCloseable) extends CloseableGauge {
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
