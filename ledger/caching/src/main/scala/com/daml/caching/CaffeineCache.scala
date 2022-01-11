// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

object CaffeineCache {

  def apply[Key <: AnyRef, Value <: AnyRef](
                                             builder: caffeine.Caffeine[_ >: Key, _ >: Value],
                                             metrics: Option[CacheMetrics],
                                           ): ConcurrentCache[Key, Value] =
    metrics match {
      case None => new SimpleCaffeineCache(builder.build[Key, Value])
      case Some(metrics) =>
        builder.recordStats(() => new DropwizardStatsCounter(metrics))
        new InstrumentedCaffeineCache(builder.build[Key, Value], metrics)
    }

  private final class SimpleCaffeineCache[Key <: AnyRef, Value <: AnyRef](
                                                                           cache: caffeine.Cache[Key, Value]
                                                                         ) extends ConcurrentCache[Key, Value] {
    override def put(key: Key, value: Value): Unit = cache.put(key, value)

    override def getIfPresent(key: Key): Option[Value] =
      Option(cache.getIfPresent(key))

    override def getOrAcquire(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))

    override def invalidate(key: Key): Unit = cache.invalidate(key)
  }

  class SimpleAsyncLoadingCache[Key <: AnyRef, Value <: AnyRef](
                                                                 cache: com.github.benmanes.caffeine.cache.AsyncLoadingCache[Key, Value],
                                                               ) extends AsyncLoadingCache[Key, Value] {

    override def get(key: Key): Future[Value] = FutureConverters.toScala(cache.get(key))

    override def invalidate(key: Key): Unit = cache.synchronous().invalidate(key)

  }

  private final class InstrumentedCaffeineCache[Key <: AnyRef, Value <: AnyRef](
                                                                                 cache: caffeine.Cache[Key, Value],
                                                                                 metrics: CacheMetrics,
                                                                               ) extends ConcurrentCache[Key, Value] {
    metrics.registerSizeGauge(() => cache.estimatedSize())
    metrics.registerWeightGauge(() =>
      cache.policy().eviction().asScala.flatMap(_.weightedSize.asScala).getOrElse(0)
    )

    private val delegate = new SimpleCaffeineCache(cache)

    override def put(key: Key, value: Value): Unit =
      delegate.put(key, value)

    override def getIfPresent(key: Key): Option[Value] =
      delegate.getIfPresent(key)

    override def getOrAcquire(key: Key, acquire: Key => Value): Value =
      delegate.getOrAcquire(key, acquire)

    override def invalidate(key: Key): Unit =
      delegate.invalidate(key)
  }


//  class InstrumentedAsyncLoadingCache[Key <: AnyRef, Value <: AnyRef](
//                                                                       cache: AsyncLoadingCache[Key, Value],
//                                                                       metrics: CacheMetrics
//                                                                     ) {
//
//
//  }


}
