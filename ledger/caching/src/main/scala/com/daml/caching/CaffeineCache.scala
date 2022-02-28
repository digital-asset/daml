// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.{CacheMetrics, Metrics}
import com.github.benmanes.caffeine.{cache => caffeine}

import scala.jdk.FutureConverters.CompletionStageOps
import scala.concurrent.Future

object CaffeineCache {

  def apply[Key <: AnyRef, Value <: AnyRef](
      builder: caffeine.Caffeine[_ >: Key, _ >: Value],
      metrics: Option[(CacheMetrics, Metrics)],
  ): ConcurrentCache[Key, Value] =
    metrics match {
      case None => new SimpleCaffeineCache(builder.build[Key, Value])
      case Some((cacheMetrics, metrics)) =>
        new InstrumentedCaffeineCache(builder.build[Key, Value], cacheMetrics, metrics)
    }

  private final class SimpleCaffeineCache[Key <: AnyRef, Value <: AnyRef](
      cache: caffeine.Cache[Key, Value]
  ) extends ConcurrentCache[Key, Value] {
    override def put(key: Key, value: Value): Unit = cache.put(key, value)

    override def getIfPresent(key: Key): Option[Value] =
      Option(cache.getIfPresent(key))

    override def getOrAcquire(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))
  }

  final class AsyncLoadingCaffeineCache[Key <: AnyRef, Value <: AnyRef](
      cache: caffeine.AsyncLoadingCache[Key, Value],
      cacheMetrics: CacheMetrics,
      metrics: Metrics,
  ) {
    installMetrics(cacheMetrics, metrics, cache.synchronous())

    def get(key: Key): Future[Value] = cache.get(key).asScala

    def invalidate(key: Key): Unit = cache.synchronous().invalidate(key)
  }

  private final class InstrumentedCaffeineCache[Key <: AnyRef, Value <: AnyRef](
      cache: caffeine.Cache[Key, Value],
      cacheMetrics: CacheMetrics,
      metrics: Metrics,
  ) extends ConcurrentCache[Key, Value] {
    installMetrics(cacheMetrics, metrics, cache)

    private val delegate = new SimpleCaffeineCache(cache)

    override def put(key: Key, value: Value): Unit =
      delegate.put(key, value)

    override def getIfPresent(key: Key): Option[Value] =
      delegate.getIfPresent(key)

    override def getOrAcquire(key: Key, acquire: Key => Value): Value =
      delegate.getOrAcquire(key, acquire)
  }

  private def installMetrics(
      cacheMetrics: CacheMetrics,
      metrics: Metrics,
      cache: caffeine.Cache[_, _],
  ): Unit =
    metrics.registerCaffeineCache(cacheMetrics, cache)
}
