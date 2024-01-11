// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.lf.transaction.GlobalKey
import com.digitalasset.canton.caching.{CaffeineCache, ConcurrentCache}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.cache.EventsByContractKeyCache.KeyUpdates
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate

object EventsByContractKeyCache {

  def build(metrics: Metrics, cacheSize: Long = 10000): EventsByContractKeyCache = {
    val cache = CaffeineCache[GlobalKey, KeyUpdates](
      com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(cacheSize),
      Some(metrics.daml.services.eventsByContractKeyCache),
    )
    new EventsByContractKeyCache(cache)
  }

  final case class KeyUpdates(
      key: GlobalKey,
      create: TransactionLogUpdate.CreatedEvent,
      archive: Option[TransactionLogUpdate.ExercisedEvent],
  )

}

class EventsByContractKeyCache(
    private[cache] val cache: ConcurrentCache[GlobalKey, KeyUpdates]
) {

  def push(batch: Vector[(GlobalKey, TransactionLogUpdate.Event)]): Unit = {
    batch.foreach({
      case (key, update: TransactionLogUpdate.CreatedEvent) =>
        cache.put(key, KeyUpdates(key, update, None))
      case (key, update: TransactionLogUpdate.ExercisedEvent) =>
        cache.getIfPresent(key).foreach { r =>
          cache.put(key, r.copy(archive = Some(update)))
        }
    })
  }

  def get(key: GlobalKey): Option[KeyUpdates] = cache.getIfPresent(key)

  def flush(): Unit = cache.invalidateAll()

}
