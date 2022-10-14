// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricDoc.MetricQualification.Debug
import com.daml.metrics.MetricHandle.Counter

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics.{Gauge, MetricRegistry}

final class CacheMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {

  @MetricDoc.Tag(
    summary = "The number of cache hits.",
    description = """When a cache lookup encounters an existing cache entry, the counter is
                    |incremented.""",
    qualification = Debug,
  )
  @MetricDoc.GroupTag(
    representative = "daml.execution.cache.<state_cache>.hits"
  )
  val hitCount: Counter = counter(prefix :+ "hits")

  @MetricDoc.Tag(
    summary = "The number of cache misses.",
    description = """When a cache lookup first encounters a missing cache entry, the counter is
                    |incremented.""",
    qualification = Debug,
  )
  @MetricDoc.GroupTag(
    representative = "daml.execution.cache.<state_cache>.misses"
  )
  val missCount: Counter = counter(prefix :+ "misses")

  @MetricDoc.Tag(
    summary = "The number of the evicted cache entries.",
    description = "When an entry is evicted from the cache, the counter is incremented.",
    qualification = Debug,
  )
  @MetricDoc.GroupTag(
    representative = "daml.execution.cache.<state_cache>.evictions"
  )
  val evictionCount: Counter = counter(prefix :+ "evictions")

  @MetricDoc.Tag(
    summary = "The sum of weights of cache entries evicted.",
    description = "The total weight of the entries evicted from the cache.",
    qualification = Debug,
  )
  @MetricDoc.GroupTag(
    representative = "daml.execution.cache.<state_cache>.evicted_weight"
  )
  val evictionWeight: Counter = counter(prefix :+ "evicted_weight")

  def registerSizeGauge(sizeGauge: Gauge[Long]): Unit =
    register(prefix :+ "size", () => sizeGauge)
  def registerWeightGauge(weightGauge: Gauge[Long]): Unit =
    register(prefix :+ "weight", () => weightGauge)

  private def register(name: MetricName, gaugeSupplier: MetricSupplier[Gauge[_]]): Unit = {
    gaugeWithSupplier(name, gaugeSupplier)
    ()
  }
}
