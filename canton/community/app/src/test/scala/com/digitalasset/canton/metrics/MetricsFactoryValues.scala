// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.testing.InMemoryMetricsFactory.{MetricsByName, MetricsState}
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{MetricName, MetricsContext}

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.language.implicitConversions

trait MetricsFactoryValues extends MetricValues {

  implicit def convertFactoryToValuable(
      factory: CantonLabeledMetricsFactory
  ): MetricsFactoryValuable = MetricsFactoryValuable(
    factory
  )

  // Not final due to scalac: "The outer reference in this type test cannot be checked at run time."
  case class MetricsFactoryValuable(factory: CantonLabeledMetricsFactory) {

    def asInMemory: InMemoryMetricsFactory = factory match {
      case inMemory: InMemoryMetricsFactory => inMemory
      case _ =>
        throw new IllegalArgumentException(s"Cannot convert $factory to in-memory factory.")
    }
  }

  implicit def inMemoryMetricToValuable[T](
      state: concurrent.Map[MetricsContext, T]
  ): InMemoryMetricValuable[T] = InMemoryMetricValuable(state)

  case class InMemoryMetricValuable[T](state: concurrent.Map[MetricsContext, T]) {
    def singleMetric: T = MetricValues.singleValueFromContexts(state.toMap)
  }

  implicit def metricStateToValuable(state: MetricsState): MetricsStateValuable =
    MetricsStateValuable(state)

  case class MetricsStateValuable(state: MetricsState) {

    def totalMetricsRegistered: Int = {
      state.gauges.size + state.asyncGauges.size + state.histograms.size + state.timers.size + state.counters.size + state.meters.size
    }

    def filteredForPrefix(metricName: MetricName): MetricsState = {
      def filteredMapForPrefix[Metric](state: MetricsByName[Metric]): MetricsByName[Metric] = {
        TrieMap.from(
          state.toMap.view.filterKeys(_.startsWith(metricName)).toMap
        )
      }

      MetricsState(
        timers = filteredMapForPrefix(state.timers),
        gauges = filteredMapForPrefix(state.gauges),
        asyncGauges = filteredMapForPrefix(state.asyncGauges),
        meters = filteredMapForPrefix(state.meters),
        counters = filteredMapForPrefix(state.counters),
        histograms = filteredMapForPrefix(state.histograms),
      )
    }

    def singleCounter(
        metricName: MetricName
    ): InMemoryMetricsFactory.InMemoryCounter = state.counters
      .getOrElse(
        metricName,
        throw new IllegalStateException(
          s"Cannot find counter with name $metricName in the metric state $state"
        ),
      )
      .singleMetric

    def singleGauge(
        metricName: MetricName
    ): InMemoryMetricsFactory.InMemoryGauge[?] = state.gauges
      .getOrElse(
        metricName,
        throw new IllegalStateException(
          s"Cannot find counter with name $metricName in the metric state $state"
        ),
      )
      .singleMetric

    def metricNames: collection.Set[MetricName] =
      state.meters.keySet ++ state.counters.keySet ++ state.gauges.keySet ++ state.asyncGauges.keySet ++ state.timers.keySet ++ state.histograms.keySet
  }
}
