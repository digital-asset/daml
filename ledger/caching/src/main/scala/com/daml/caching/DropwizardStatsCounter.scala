// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.stats.{CacheStats, StatsCounter}

private[caching] final class DropwizardStatsCounter(
    metrics: CacheMetrics
) extends StatsCounter {

  override def recordHits(newHits: Int): Unit =
    metrics.hitCount.inc(newHits.toLong)

  override def recordMisses(newMisses: Int): Unit =
    metrics.missCount.inc(newMisses.toLong)

  override def recordLoadSuccess(loadTimeNanos: Long): Unit = ()

  override def recordLoadFailure(loadTimeNanos: Long): Unit = ()

  override def recordEviction(weight: Int, cause: RemovalCause): Unit = {
    metrics.evictionCount.inc()
    metrics.evictionWeight.inc(weight.toLong)
  }

  override def snapshot(): CacheStats = CacheStats.empty

}
