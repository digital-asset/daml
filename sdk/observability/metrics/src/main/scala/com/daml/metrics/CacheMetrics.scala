// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}
import com.daml.scalautil.Statement.discard

final class CacheMetrics(name: String, factory: LabeledMetricsFactory) {

  private val prefix = MetricName.Daml :+ "cache"

  private implicit val mc: MetricsContext = MetricsContext("name" -> name)

  @MetricDoc.Tag(
    summary = "The number of cache hits.",
    description = """When a cache lookup encounters an existing cache entry, the counter is
                    |incremented.""",
    qualification = Debug,
    labelsWithDescription = Map("name" -> "The cache for which the metrics are registered."),
  )
  val hitCount: Counter = factory.counter(prefix :+ "hits")

  @MetricDoc.Tag(
    summary = "The number of cache misses.",
    description = """When a cache lookup first encounters a missing cache entry, the counter is
                    |incremented.""",
    qualification = Debug,
    labelsWithDescription = Map("name" -> "The cache for which the metrics are registered."),
  )
  val missCount: Counter = factory.counter(prefix :+ "misses")

  @MetricDoc.Tag(
    summary = "The number of the evicted cache entries.",
    description = "When an entry is evicted from the cache, the counter is incremented.",
    qualification = Debug,
    labelsWithDescription = Map("name" -> "The cache for which the metrics are registered."),
  )
  val evictionCount: Counter = factory.counter(prefix :+ "evictions")

  @MetricDoc.Tag(
    summary = "The sum of weights of cache entries evicted.",
    description = "The total weight of the entries evicted from the cache.",
    qualification = Debug,
    labelsWithDescription = Map("name" -> "The cache for which the metrics are registered."),
  )
  val evictionWeight: Counter = factory.counter(prefix :+ "evicted_weight")

  def registerSizeGauge(sizeSupplier: () => Long): Unit = discard {
    factory.gaugeWithSupplier(prefix :+ "size", sizeSupplier)
  }
  def registerWeightGauge(weightSupplier: () => Long): Unit = discard {
    factory.gaugeWithSupplier(prefix :+ "weight", weightSupplier)
  }

}
