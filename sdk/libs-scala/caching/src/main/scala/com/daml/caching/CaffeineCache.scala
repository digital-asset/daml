// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import com.github.benmanes.caffeine.{cache => caffeine}

import java.util.concurrent.{CompletableFuture, Executor}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.jdk.OptionConverters.{RichOptional, RichOptionalLong}
import scala.util.{Failure, Success}

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

    override def putAll(mappings: Map[Key, Value]): Unit =
      cache.putAll(mappings.asJava)

    override def getIfPresent(key: Key): Option[Value] =
      Option(cache.getIfPresent(key))

    override def getOrAcquire(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))

    override def invalidateAll(): Unit = cache.invalidateAll()
  }

  final class AsyncLoadingCaffeineCache[Key <: AnyRef, Value <: AnyRef](
      cache: caffeine.AsyncLoadingCache[Key, Value],
      cacheMetrics: CacheMetrics,
  ) {
    installMetrics(cacheMetrics, cache.synchronous())

    def get(key: Key): Future[Value] = cache.get(key).asScala

    def invalidate(key: Key): Unit = cache.synchronous().invalidate(key)

    def invalidateAll(): Unit = cache.synchronous().invalidateAll()
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

  private def installMetrics[Key <: AnyRef, Value <: AnyRef](
      metrics: CacheMetrics,
      cache: caffeine.Cache[Key, Value],
  ): Unit = {
    metrics.registerSizeGauge(() => cache.estimatedSize())
    metrics.registerWeightGauge(() =>
      cache.policy().eviction().toScala.flatMap(_.weightedSize.toScala).getOrElse(0)
    )
  }

  class FutureAsyncCacheLoader[K, V](func: K => Future[V])(implicit
      executionContext: ExecutionContext
  ) extends AsyncCacheLoader[K, V] {
    override def asyncLoad(key: K, executor: Executor): CompletableFuture[_ <: V] = {
      val cf = new CompletableFuture[V]
      func(key).onComplete {
        case Success(value) => cf.complete(value)
        case Failure(e) => cf.completeExceptionally(e)
      }
      cf
    }
  }

}
