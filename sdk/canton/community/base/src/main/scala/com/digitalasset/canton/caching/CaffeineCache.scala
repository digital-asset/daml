// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.cache as caffeine

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.{RichOptional, RichOptionalLong}

object CaffeineCache {

  def apply[Key <: AnyRef, Value <: AnyRef](
      builder: caffeine.Caffeine[_ >: Key, _ >: Value],
      metrics: Option[CacheMetrics],
  ): ConcurrentCache[Key, Value] =
    metrics match {
      case None => new SimpleCaffeineCache(builder.build[Key, Value])
      case Some(metrics) =>
        builder.recordStats(() => new StatsCounterMetrics(metrics))
        new InstrumentedCaffeineCache(builder.build[Key, Value], metrics)
    }

  private final class SimpleCaffeineCache[Key <: AnyRef, Value <: AnyRef](
      cache: caffeine.Cache[Key, Value]
  ) extends ConcurrentCache[Key, Value] {
    override def put(key: Key, value: Value): Unit = cache.put(key, value)

    override def putAll(mappings: Map[Key, Value]): Unit =
      cache.putAll(mappings.asJava)

    override def getIfPresent(key: Key): Option[Value] =
      Option(cache.getIfPresent(key))

    override def getOrAcquire(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))

    override def invalidateAll(): Unit = cache.invalidateAll()
  }

  private final class InstrumentedCaffeineCache[Key <: AnyRef, Value <: AnyRef](
      cache: caffeine.Cache[Key, Value],
      metrics: CacheMetrics,
  ) extends ConcurrentCache[Key, Value] {
    installMetrics(metrics, cache)

    private val delegate = new SimpleCaffeineCache(cache)

    override def put(key: Key, value: Value): Unit =
      delegate.put(key, value)

    override def putAll(mappings: Map[Key, Value]): Unit =
      delegate.putAll(mappings)

    override def getIfPresent(key: Key): Option[Value] =
      delegate.getIfPresent(key)

    override def getOrAcquire(key: Key, acquire: Key => Value): Value =
      delegate.getOrAcquire(key, acquire)

    override def invalidateAll(): Unit = delegate.invalidateAll()
  }

  private[caching] def installMetrics(
      metrics: CacheMetrics,
      cache: caffeine.Cache[?, ?],
  ): Unit = {
    metrics.registerSizeGauge(() => cache.estimatedSize())
    metrics.registerWeightGauge(() =>
      cache.policy().eviction().toScala.flatMap(_.weightedSize.toScala).getOrElse(0)
    )
  }
}
