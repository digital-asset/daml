// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Counter, Factory}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}

final class CacheMetrics(val prefix: MetricName, val factory: Factory) {

  @MetricDoc.Tag(
    summary = "The number of cache hits.",
    description = """When a cache lookup encounters an existing cache entry, the counter is
                    |incremented.""",
    qualification = Debug,
  )
  val hitCount: Counter = factory.counter(prefix :+ "hits")

  @MetricDoc.Tag(
    summary = "The number of cache misses.",
    description = """When a cache lookup first encounters a missing cache entry, the counter is
                    |incremented.""",
    qualification = Debug,
  )
  val missCount: Counter = factory.counter(prefix :+ "misses")

  @MetricDoc.Tag(
    summary = "The number of the evicted cache entries.",
    description = "When an entry is evicted from the cache, the counter is incremented.",
    qualification = Debug,
  )
  val evictionCount: Counter = factory.counter(prefix :+ "evictions")

  @MetricDoc.Tag(
    summary = "The sum of weights of cache entries evicted.",
    description = "The total weight of the entries evicted from the cache.",
    qualification = Debug,
  )
  val evictionWeight: Counter = factory.counter(prefix :+ "evicted_weight")

  def registerSizeGauge(sizeSupplier: () => Long): Unit =
    factory.gaugeWithSupplier(prefix :+ "size", sizeSupplier)(MetricsContext.Empty)
  def registerWeightGauge(weightSupplier: () => Long): Unit =
    factory.gaugeWithSupplier(prefix :+ "weight", weightSupplier)(MetricsContext.Empty)

}
