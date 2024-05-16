// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricName

import scala.collection.concurrent.TrieMap

/** Bucket boundary definitions for histograms
  *
  * @param name Instrument name that may contain the wildcard characters * and ?
  * @param bucketBoundaries The boundaries of the histogram buckets
  */
final case class HistogramDefinition(name: String, bucketBoundaries: Seq[Double]) {

  private lazy val hasWildcards = name.contains("*") || name.contains("?")
  private lazy val asRegex = name.replace("*", ".*").replace("?", ".")

  def matches(metric: MetricName): Boolean = {
    if (hasWildcards) {
      metric.matches(asRegex)
    } else { metric.toString == name }
  }
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
