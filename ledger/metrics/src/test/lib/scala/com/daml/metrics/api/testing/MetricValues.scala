// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.testing

import com.codahale.metrics.Snapshot
import com.daml.metrics.api.MetricHandle.{Counter, Histogram, Meter, Timer}
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.dropwizard.{
  DropwizardCounter,
  DropwizardHistogram,
  DropwizardMeter,
  DropwizardTimer,
}
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{
  InMemoryHistogram,
  InMemoryMeter,
  InMemoryTimer,
}

trait MetricValues {

  import scala.language.implicitConversions

  implicit def convertCounterToValuable(counter: Counter): CounterValues = new CounterValues(
    counter
  )

  implicit def convertMeterToValuable(meter: Meter): MeterValues = new MeterValues(
    meter
  )

  implicit def convertHistogramToValuable(histogram: Histogram): HistogramValues =
    new HistogramValues(
      histogram
    )

  implicit def convertTimerToValuable(timer: Timer): TimerValues =
    new TimerValues(
      timer
    )

  class CounterValues(counter: Counter) {

    def value: Long = counter match {
      case DropwizardCounter(_, metric) => metric.getCount
      case other =>
        throw new IllegalArgumentException(s"Value not supported for $other")
    }
  }

  class MeterValues(meter: Meter) {

    def value: Long = meter match {
      case DropwizardMeter(_, metric) => metric.getCount
      case other =>
        throw new IllegalArgumentException(s"Value not supported for $other")
    }

    def valueWithContext: Map[MetricsContext, Long] = meter match {
      case meter: InMemoryMeter =>
        meter.markers.view.mapValues(_.get()).toMap
      case other =>
        throw new IllegalArgumentException(s"Value not supported by $other")
    }

  }

  class HistogramValues(histogram: Histogram) {

    def snapshot: Snapshot = histogram match {
      case DropwizardHistogram(_, metric) => metric.getSnapshot
      case other =>
        throw new IllegalArgumentException(s"Snapshot not supported for $other")
    }

    def valuesWithContext: Map[MetricsContext, Seq[Long]] = histogram match {
      case histogram: InMemoryHistogram =>
        histogram.values.toMap
      case other =>
        throw new IllegalArgumentException(s"Values not supported for $other")
    }

  }

  class TimerValues(timer: Timer) {

    def snapshot: Snapshot = timer match {
      case DropwizardTimer(_, metric) => metric.getSnapshot
      case other =>
        throw new IllegalArgumentException(s"Snapshot not supported for $other")
    }

    def getCount: Long = timer match {
      case DropwizardTimer(_, metric) => metric.getCount
      case other =>
        throw new IllegalArgumentException(s"Count not supported for $other")
    }

    def getCounts: Map[MetricsContext, Long] = timer match {
      case timer: InMemoryTimer =>
        timer.runTimers.toMap.view.mapValues(_.get().toLong).toMap
      case other =>
        throw new IllegalArgumentException(s"Counts not supported for $other")
    }

  }

}
