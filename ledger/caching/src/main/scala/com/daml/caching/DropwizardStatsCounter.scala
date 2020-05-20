// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.cache.stats.{CacheStats, StatsCounter}

private[caching] final class DropwizardStatsCounter(
    metrics: CacheMetrics,
) extends StatsCounter {

  override def recordHits(i: Int): Unit =
    metrics.hitCount.inc(i.toLong)

  override def recordMisses(i: Int): Unit =
    metrics.missCount.inc(i.toLong)

  override def recordLoadSuccess(l: Long): Unit =
    metrics.loadSuccessCount.inc(l)

  override def recordLoadFailure(l: Long): Unit =
    metrics.loadFailureCount.inc(l)

  override def recordEviction(): Unit = {
    metrics.evictionCount.inc()
    metrics.evictionWeight.inc()
  }

  override def snapshot(): CacheStats =
    new CacheStats(
      metrics.hitCount.getCount,
      metrics.missCount.getCount,
      metrics.loadSuccessCount.getCount,
      metrics.loadFailureCount.getCount,
      metrics.totalLoadTime.getCount,
      metrics.evictionCount.getCount,
      metrics.evictionWeight.getCount,
    )

}
