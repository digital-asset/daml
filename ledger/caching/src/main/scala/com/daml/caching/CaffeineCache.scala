// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.codahale.metrics.Gauge
import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

import scala.compat.java8.OptionConverters._

sealed class CaffeineCache[Key, Value] private[caching] (builder: caffeine.Caffeine[Key, Value])
    extends Cache[Key, Value] {

  private val cache = init(builder)

  protected def init(builder: caffeine.Caffeine[Key, Value]): caffeine.Cache[Key, Value] =
    builder.build()

  override def put(key: Key, value: Value): Unit = cache.put(key, value)

  override def get(key: Key, acquire: Key => Value): Value =
    cache.get(key, key => acquire(key))

  override def getIfPresent(key: Key): Option[Value] =
    Option(cache.getIfPresent(key))

}

final class InstrumentedCaffeineCache[Key, Value] private[caching] (
    builder: caffeine.Caffeine[Key, Value],
    metrics: CacheMetrics,
) extends CaffeineCache[Key, Value](builder) {

  override protected def init(
      builder: caffeine.Caffeine[Key, Value],
  ): caffeine.Cache[Key, Value] = {
    val cache = super.init(builder.recordStats(() => new DropwizardStatsCounter(metrics)))
    metrics.registerSizeGauge(size(cache))
    metrics.registerWeightGauge(weight(cache))
    cache
  }

  private def size(cache: caffeine.Cache[_, _]): Gauge[Long] =
    () => cache.estimatedSize()

  private def weight(cache: caffeine.Cache[_, _]): Gauge[Long] =
    () => cache.policy().eviction().asScala.flatMap(_.weightedSize.asScala).getOrElse(0)

}
