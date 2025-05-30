// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.HistogramDefinition.AggregationType
import com.daml.metrics.api.MetricName

import scala.collection.concurrent.TrieMap

/** Bucket boundary definitions for histograms
  *
  * @param name Instrument name that may contain the wildcard characters * and ?
  * @param aggregation The aggregation type for the boundaries of the histogram buckets
  */
final case class HistogramDefinition(
    name: String,
    aggregation: AggregationType,
    viewName: Option[String] = None,
) {

  private lazy val hasWildcards = name.contains("*") || name.contains("?")
  private lazy val asRegex = name.replace("*", ".*").replace("?", ".")

  def matches(metric: MetricName): Boolean = {
    if (hasWildcards) {
      metric.matches(asRegex)
    } else { metric.toString == name }
  }
}

object HistogramDefinition {
  sealed trait AggregationType
  final case class Buckets(boundaries: Seq[Double]) extends AggregationType
  final case class Exponential(maxBuckets: Int, maxScale: Int) extends AggregationType
}

/** Metrics filter */
final case class MetricsFilterConfig(
    startsWith: String = "",
    contains: String = "",
    endsWith: String = "",
) {
  def matches(name: String): Boolean =
    name.startsWith(startsWith) && name.contains(contains) && name.endsWith(endsWith)
}

/** Caching filter to filter metrics by name */
class MetricsFilter(filters: Seq[MetricsFilterConfig]) {

  // cache the result of the filter for each metric name so we don't have to traverse lists all the time
  private val computedFilters = TrieMap[String, Boolean]()

  def includeMetric(name: String): Boolean = {
    if (name.isEmpty) false
    else if (filters.isEmpty) true
    else computedFilters.getOrElseUpdate(name, filters.exists(_.matches(name)))
  }

}
