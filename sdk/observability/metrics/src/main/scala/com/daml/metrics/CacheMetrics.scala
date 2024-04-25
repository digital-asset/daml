// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricsContext}
import com.daml.scalautil.Statement.discard

final class CacheMetrics(cacheName: String, factory: LabeledMetricsFactory) {

  private val prefix = MetricName.Daml :+ "cache"
  private implicit val mc: MetricsContext = MetricsContext("cache" -> cacheName)

  val hitCount: Counter = factory.counter(
    MetricInfo(
      prefix :+ "hits",
      summary = "The number of cache hits.",
      description = """When a cache lookup encounters an existing cache entry, the counter is
                    |incremented.""",
      qualification = Debug,
    )
  )

  val missCount: Counter = factory.counter(
    MetricInfo(
      prefix :+ "misses",
      summary = "The number of cache misses.",
      description = """When a cache lookup first encounters a missing cache entry, the counter is
                    |incremented.""",
      qualification = Debug,
    )
  )

  val evictionCount: Counter = factory.counter(
    MetricInfo(
      prefix :+ "evictions",
      summary = "The number of the evicted cache entries.",
      description = "When an entry is evicted from the cache, the counter is incremented.",
      qualification = Debug,
    )
  )

  val evictionWeight: Counter = factory.counter(
    MetricInfo(
      prefix :+ "evicted_weight",
      summary = "The sum of weights of cache entries evicted.",
      description = "The total weight of the entries evicted from the cache.",
      qualification = Debug,
    )
  )

  def registerSizeGauge(sizeSupplier: () => Long): Unit = discard {
    factory.gaugeWithSupplier(
      MetricInfo(prefix :+ "size", summary = "The size of the cache", qualification = Debug),
      sizeSupplier,
    )
  }
  def registerWeightGauge(weightSupplier: () => Long): Unit = discard {
    factory.gaugeWithSupplier(
      MetricInfo(
        prefix :+ "weight",
        summary = "Approximate weight of the cached elements",
        qualification = Debug,
      ),
      weightSupplier,
    )
  }

}
