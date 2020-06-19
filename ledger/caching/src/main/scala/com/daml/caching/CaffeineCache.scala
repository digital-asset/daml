// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.codahale.metrics.Gauge
import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

import scala.compat.java8.OptionConverters._

sealed class CaffeineCache[Key <: AnyRef, Value <: AnyRef] private[caching] (
    cache: caffeine.Cache[Key, Value]
) extends Cache[Key, Value] {

  override def put(key: Key, value: Value): Unit = cache.put(key, value)

  override def get(key: Key, acquire: Key => Value): Value =
    cache.get(key, key => acquire(key))

  override def getIfPresent(key: Key): Option[Value] =
    Option(cache.getIfPresent(key))

}

final class InstrumentedCaffeineCache[Key <: AnyRef, Value <: AnyRef] private[caching] (
    cache: caffeine.Cache[Key, Value],
    metrics: CacheMetrics,
) extends CaffeineCache[Key, Value](cache) {

  metrics.registerSizeGauge(size(cache))
  metrics.registerWeightGauge(weight(cache))

  private def size(cache: caffeine.Cache[_, _]): Gauge[Long] =
    () => cache.estimatedSize()

  private def weight(cache: caffeine.Cache[_, _]): Gauge[Long] =
    () => cache.policy().eviction().asScala.flatMap(_.weightedSize.asScala).getOrElse(0)

}

object CaffeineCache {
  def apply[Key <: AnyRef, Value <: AnyRef](
      builder: caffeine.Caffeine[_ >: Key, _ >: Value],
      metrics: Option[CacheMetrics],
  ): CaffeineCache[Key, Value] = {
    val cache = builder.build[Key, Value]
    metrics.fold(new CaffeineCache(cache))(new InstrumentedCaffeineCache(cache, _))
  }
}
