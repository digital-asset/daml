// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.daml.metrics.MetricHandle.Counter

final class CacheMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  val hitCount: Counter = counter(prefix :+ "hits")
  val missCount: Counter = counter(prefix :+ "misses")
  val evictionCount: Counter = counter(prefix :+ "evictions")
  val evictionWeight: Counter = counter(prefix :+ "evicted_weight")

  def registerSizeGauge(sizeGauge: Gauge[Long]): Unit =
    gaugeWithSupplier(prefix :+ "size", () => () => sizeGauge.getValue)
  def registerWeightGauge(weightGauge: Gauge[Long]): Unit =
    gaugeWithSupplier(prefix :+ "weight", () => () => weightGauge.getValue)

}
