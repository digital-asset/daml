// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.Counter
import com.daml.metrics.api.dropwizard.DropwizardFactory
import com.daml.metrics.api.{MetricDoc, MetricName}

final class CacheMetrics(val prefix: MetricName, override val registry: MetricRegistry)
    extends DropwizardFactory {

  @MetricDoc.Tag(
    summary = "The number of cache hits.",
    description = """When a cache lookup encounters an existing cache entry, the counter is
                    |incremented.""",
    qualification = Debug,
  )
  val hitCount: Counter = counter(prefix :+ "hits")

  @MetricDoc.Tag(
    summary = "The number of cache misses.",
    description = """When a cache lookup first encounters a missing cache entry, the counter is
                    |incremented.""",
    qualification = Debug,
  )
  val missCount: Counter = counter(prefix :+ "misses")

  @MetricDoc.Tag(
    summary = "The number of the evicted cache entries.",
    description = "When an entry is evicted from the cache, the counter is incremented.",
    qualification = Debug,
  )
  val evictionCount: Counter = counter(prefix :+ "evictions")

  @MetricDoc.Tag(
    summary = "The sum of weights of cache entries evicted.",
    description = "The total weight of the entries evicted from the cache.",
    qualification = Debug,
  )
  val evictionWeight: Counter = counter(prefix :+ "evicted_weight")

  def registerSizeGauge(sizeSupplier: () => Long): Unit =
    gaugeWithSupplier(prefix :+ "size", sizeSupplier)
  def registerWeightGauge(weightSupplier: () => Long): Unit =
    gaugeWithSupplier(prefix :+ "weight", weightSupplier)

}
