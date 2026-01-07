// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance

import com.codahale.metrics.{Histogram, Meter, Metric, MetricRegistry}

object BftMetrics {

  type MetricName = String

  @volatile var pendingReads = 0L

  final case class NamedMetric[M <: Metric](
      name: MetricName,
      metric: M,
  )

  def startedWriteMeters(
      metrics: MetricRegistry,
      nodeIndices: Iterable[Int],
  ): Iterable[NamedMetric[Meter]] =
    namedMetrics(
      metrics,
      nodeIndices,
      globalMetricName = "global.writes.started.meter",
      nodeMetricNameBuilder = nodeIndex => s"node$nodeIndex.writes.started.meter",
      metricExtractor = (metrics, name) => metrics.meter(name),
    )

  def successfulWriteMeters(
      metrics: MetricRegistry,
      nodeIndices: Iterable[Int],
  ): Iterable[NamedMetric[Meter]] =
    namedMetrics(
      metrics,
      nodeIndices,
      globalMetricName = "global.writes.successful.meter",
      nodeMetricNameBuilder = nodeIndex => s"node$nodeIndex.writes.successful.meter",
      metricExtractor = (metrics, name) => metrics.meter(name),
    )

  def failedWriteMeters(
      metrics: MetricRegistry,
      nodeIndices: Iterable[Int],
  ): Iterable[NamedMetric[Meter]] =
    namedMetrics(
      metrics,
      nodeIndices,
      globalMetricName = "global.writes.failed.meter",
      nodeMetricNameBuilder = nodeIndex => s"node$nodeIndex.writes.failed.meter",
      metricExtractor = (metrics, name) => metrics.meter(name),
    )

  def writeNanosHistograms(
      metrics: MetricRegistry,
      nodeIndices: Iterable[Int],
  ): Iterable[NamedMetric[Histogram]] =
    namedMetrics(
      metrics,
      nodeIndices,
      globalMetricName = "global.writes.duration.histogram",
      nodeMetricNameBuilder = nodeIndex => s"node$nodeIndex.writes.duration.histogram",
      metricExtractor = (metrics, name) => metrics.histogram(name),
    )

  def readMeters(
      metrics: MetricRegistry,
      nodeIndices: Iterable[Int],
  ): Iterable[NamedMetric[Meter]] =
    namedMetrics(
      metrics,
      nodeIndices,
      globalMetricName = "global.reads.meter",
      nodeMetricNameBuilder = nodeIndex => s"node$nodeIndex.reads.meter",
      metricExtractor = (metrics, name) => metrics.meter(name),
    )

  def roundTripNanosHistogram(metrics: MetricRegistry): NamedMetric[Histogram] = {
    val name = "global.roundtrips.duration.histogram"
    NamedMetric(name, metrics.histogram(name))
  }

  private def namedMetrics[M <: Metric](
      metrics: MetricRegistry,
      nodeIndices: Iterable[Int],
      globalMetricName: MetricName,
      nodeMetricNameBuilder: Int => MetricName,
      metricExtractor: (MetricRegistry, MetricName) => M,
  ): Iterable[NamedMetric[M]] =
    List.newBuilder
      .addAll(nodeIndices.map(nodeMetricNameBuilder))
      .addOne(globalMetricName)
      .result()
      .map(name => NamedMetric(name, metricExtractor(metrics, name)))
}
