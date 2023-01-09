// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.testing

import com.daml.metrics.api.MetricHandle.{Counter, Histogram, Meter, Timer}
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{
  InMemoryCounter,
  InMemoryHistogram,
  InMemoryMeter,
  InMemoryTimer,
}
import com.daml.metrics.api.{MetricName, MetricsContext}

trait MetricValues {

  import scala.language.implicitConversions

  implicit def convertInMemoryFactoryToValuable(
      factory: InMemoryMetricsFactory
  ): InMemoryMetricFactoryValues = new InMemoryMetricFactoryValues(
    factory
  )

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

  class InMemoryMetricFactoryValues(factory: InMemoryMetricsFactory) {

    def asyncGaugeValues(labelFilter: LabelFilter*): Map[MetricName, Any] = {
      factory.asyncGauges
        .filter { case ((_, metricContext), _) =>
          labelFilter.forall(filter => metricContext.labels.get(filter.name).contains(filter.value))
        }
        .map { case ((metricName, _), valueProvider) =>
          metricName -> valueProvider()
        }
        .toMap
    }

  }

  class CounterValues(counter: Counter) {

    def value: Long = counter match {
      case counter: InMemoryCounter =>
        singleValueFromContexts(counter.markers.toMap).get()
      case other =>
        throw new IllegalArgumentException(s"Value not supported for $other")
    }
  }

  class MeterValues(meter: Meter) {

    def value: Long = meter match {
      case meter: InMemoryMeter =>
        val contextWithValues = meter.markers.view.mapValues(_.get()).toMap
        singleValueFromContexts(contextWithValues)
      case other =>
        throw new IllegalArgumentException(s"Value not supported for $other")
    }

    def valuesWithContext: Map[MetricsContext, Long] = meter match {
      case meter: InMemoryMeter =>
        meter.markers.view.mapValues(_.get()).toMap
      case other =>
        throw new IllegalArgumentException(s"Value not supported by $other")
    }

    def valueFilteredOnLabels(labelFilters: LabelFilter*): Long =
      singleValueFromContextsFilteredOnLabels(valuesWithContext, labelFilters: _*)

  }

  class HistogramValues(histogram: Histogram) {

    def values: Seq[Long] = histogram match {
      case histogram: InMemoryHistogram =>
        singleValueFromContexts(histogram.values.toMap)
      case other =>
        throw new IllegalArgumentException(s"Values not supported for $other")
    }

    def valuesWithContext: Map[MetricsContext, Seq[Long]] = histogram match {
      case histogram: InMemoryHistogram =>
        histogram.values.toMap
      case other =>
        throw new IllegalArgumentException(s"Values not supported for $other")
    }

    def valuesFilteredOnLabels(labelFilters: LabelFilter*): Seq[Long] =
      singleValueFromContextsFilteredOnLabels(valuesWithContext, labelFilters: _*)
  }

  class TimerValues(timer: Timer) {

    def count: Long = timer match {
      case timer: InMemoryTimer =>
        singleValueFromContexts(timer.data.values.toMap.view.mapValues(_.size.toLong).toMap)
      case other =>
        throw new IllegalArgumentException(s"Count not supported for $other")
    }

    def countsWithContext: Map[MetricsContext, Long] = timer match {
      case timer: InMemoryTimer =>
        timer.data.values.toMap.view.mapValues(_.size.toLong).toMap
      case other =>
        throw new IllegalArgumentException(s"Counts not supported for $other")
    }

    def values: Seq[Long] = timer match {
      case timer: InMemoryTimer =>
        singleValueFromContexts(timer.data.values.toMap)
      case other =>
        throw new IllegalArgumentException(s"Count not supported for $other")
    }

    def valuesWithContext: Map[MetricsContext, Seq[Long]] = timer match {
      case timer: InMemoryTimer =>
        timer.data.values.toMap
      case other =>
        throw new IllegalArgumentException(s"Values not supported for $other")
    }

    def valuesFilteredOnLabels(labelFilters: LabelFilter*): Seq[Long] =
      singleValueFromContextsFilteredOnLabels(valuesWithContext, labelFilters: _*)
  }

  case class LabelFilter(name: String, value: String)

  private def singleValueFromContextsFilteredOnLabels[T](
      contextToValueMapping: Map[MetricsContext, T],
      labelFilters: LabelFilter*
  ): T = {
    val matchingFilters = labelFilters.foldLeft(contextToValueMapping) { (acc, labelFilter) =>
      acc.filter(labelFilter.value == _._1.labels.getOrElse(labelFilter.name, null))
    }
    singleValueFromContexts(matchingFilters)
  }

  private def singleValueFromContexts[T](
      contextToValueMapping: Map[MetricsContext, T]
  ) = if (contextToValueMapping.size == 1)
    contextToValueMapping.head._2
  else
    throw new IllegalArgumentException("Cannot get value with multi context metrics.")

}
