// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

import scala.compat.java8.OptionConverters._

object CaffeineCache {

  def apply[Key <: AnyRef, Value <: AnyRef](
      builder: caffeine.Caffeine[_ >: Key, _ >: Value],
      metrics: Option[CacheMetrics],
  ): Cache[Key, Value] = {
    val cache = builder.build[Key, Value]
    metrics match {
      case None => new SimpleCaffeineCache(cache)
      case Some(metrics) => new InstrumentedCaffeineCache(cache, metrics)
    }
  }

  private final class SimpleCaffeineCache[Key <: AnyRef, Value <: AnyRef](
      cache: caffeine.Cache[Key, Value],
  ) extends Cache[Key, Value] {
    override def put(key: Key, value: Value): Unit = cache.put(key, value)

    override def get(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))

    override def getIfPresent(key: Key): Option[Value] =
      Option(cache.getIfPresent(key))
  }

  private final class InstrumentedCaffeineCache[Key <: AnyRef, Value <: AnyRef](
      cache: caffeine.Cache[Key, Value],
      metrics: CacheMetrics,
  ) extends Cache[Key, Value] {
    metrics.registerSizeGauge(() => cache.estimatedSize())
    metrics.registerWeightGauge(() =>
      cache.policy().eviction().asScala.flatMap(_.weightedSize.asScala).getOrElse(0))

    private val delegate = new SimpleCaffeineCache(cache)

    override def get(key: Key, acquire: Key => Value): Value =
      delegate.get(key, acquire)

    override def getIfPresent(key: Key): Option[Value] =
      delegate.getIfPresent(key)

    override def put(key: Key, value: Value): Unit =
      delegate.put(key, value)
  }

}
