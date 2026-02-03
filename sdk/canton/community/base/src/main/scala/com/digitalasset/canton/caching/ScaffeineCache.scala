// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import cats.syntax.flatMap.*
import cats.{FlatMap, Functor}
import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.CacheMetrics
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.blemale.scaffeine.{AsyncCache, AsyncLoadingCache, Scaffeine}

import java.util.concurrent.CompletableFuture
import java.util.function.BiFunction
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala
import scala.jdk.javaapi.FutureConverters
import scala.util.chaining.scalaUtilChainingOps

object ScaffeineCache {

  def buildMappedAsync[K, V](
      cache: Scaffeine[Any, Any],
      metrics: Option[CacheMetrics] = None,
      evictionListener: Option[(K, V, RemovalCause) => Unit] = None,
  )(
      tracedLogger: TracedLogger,
      context: String,
  ): TunnelledAsyncCacheImpl[K, V] = {
    val contextWithLoggerName = s"${tracedLogger.underlying.getName}:$context"
    val cacheWithMetrics =
      metrics.fold(cache)(m => cache.recordStats(() => new StatsCounterMetrics(m)))
    val cacheWithEviction = evictionListener match {
      case Some(listener) =>
        cacheWithMetrics.evictionListener[K, V](listener)
      case None =>
        cacheWithMetrics
    }
    val asyncCache = cacheWithEviction.buildAsync[K, V]()
    metrics.foreach(CaffeineCache.installMetrics(_, asyncCache.underlying.synchronous()))
    new TunnelledAsyncCacheImpl[K, V](
      asyncCache,
      tracedLogger,
      contextWithLoggerName,
    )
  }

  def buildAsync[F[_], K, V](
      cache: Scaffeine[Any, Any],
      loader: K => F[V],
      allLoader: Option[Iterable[K] => F[Map[K, V]]] = None,
      metrics: Option[CacheMetrics] = None,
  )(
      tracedLogger: TracedLogger,
      context: String,
  )(implicit tunnel: EffectTunnel[F, Future]): TunnelledAsyncLoadingCache[F, K, V] = {
    // Add the logger name to the context so that we get the usual log line attribution
    // when the tunnelled effect gets logged by `com.github.benmanes.caffeine.cache.LocalAsyncCache.handleCompletion`,
    // which bypasses our logger factories.
    val contextWithLoggerName = s"${tracedLogger.underlying.getName}:$context"
    val cacheWithMetrics =
      metrics.fold(cache)(m => cache.recordStats(() => new StatsCounterMetrics(m)))
    val asyncLoadingCache = cacheWithMetrics.buildAsyncFuture[K, V](
      loader = key => tunnel.enter(loader(key), contextWithLoggerName),
      allLoader = allLoader.map(loader => keys => tunnel.enter(loader(keys), contextWithLoggerName)),
    )
    metrics.foreach(CaffeineCache.installMetrics(_, asyncLoadingCache.underlying.synchronous()))
    new TunnelledAsyncLoadingCache[F, K, V](
      asyncLoadingCache,
      tracedLogger,
      tunnel,
      contextWithLoggerName,
    )
  }

  def buildTracedAsync[F[_], K, V](
      cache: Scaffeine[Any, Any],
      loader: TraceContext => K => F[V],
      allLoader: Option[TraceContext => Iterable[K] => F[Map[K, V]]] = None,
      metrics: Option[CacheMetrics] = None,
  )(
      tracedLogger: TracedLogger,
      context: String,
  )(implicit tunnel: EffectTunnel[F, Future], F: Functor[F]): TracedAsyncLoadingCache[F, K, V] = {
    val asyncCache = ScaffeineCache.buildAsync[F, Traced[K], V](
      cache,
      loader = _.withTraceContext(loader),
      allLoader = allLoader.map(tracedAllLoader(tracedLogger, _)),
      metrics,
    )(tracedLogger, context)
    new TracedAsyncLoadingCache[F, K, V](asyncCache)
  }

  private def tracedAllLoader[F[_], K, V](
      tracedLogger: TracedLogger,
      allLoader: TraceContext => Iterable[K] => F[Map[K, V]],
  )(implicit F: Functor[F]): Iterable[Traced[K]] => F[Map[Traced[K], V]] = { tracedKeys =>
    val traceContext = TraceContext.ofBatch("cache_batch_load")(tracedKeys)(tracedLogger)
    val keys = tracedKeys.map(_.unwrap)
    F.map(allLoader(traceContext)(keys))(_.map { case (key, value) =>
      Traced(key)(traceContext) -> value
    })
  }

  trait TunnelledAsyncCache[K, V] {
    def getFuture(
        key: K,
        mappingFunction: K => FutureUnlessShutdown[V],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[V]

    def invalidate(key: K): Unit

    def put(key: K, value: V): Unit

    def invalidateAll(keys: Iterable[K]): Unit

    def invalidateAll(): Unit

    def cleanUp(): Unit

    def getIfPresentSync(key: K): Option[V]
  }

  class TunnelledAsyncCacheImpl[K, V] private[ScaffeineCache] (
      underlying: AsyncCache[K, V],
      tracedLogger: TracedLogger,
      context: String,
  ) extends TunnelledAsyncCache[K, V] {
    implicit private[this] val ec: ExecutionContext = DirectExecutionContext(tracedLogger)
    val tunnel: EffectTunnel[FutureUnlessShutdown, Future] =
      EffectTunnel.effectTunnelFutureUnlessShutdown
    def getFuture(
        key: K,
        mappingFunction: K => FutureUnlessShutdown[V],
    )(implicit _traceContext: TraceContext): FutureUnlessShutdown[V] =
      tunnel.exit(underlying.getFuture(key, k => tunnel.enter(mappingFunction(k), context)))

    def invalidate(key: K): Unit = underlying.synchronous().invalidate(key)

    def put(key: K, value: V): Unit = underlying.put(key, Future.successful(value))

    def invalidateAll(keys: Iterable[K]): Unit = underlying.synchronous().invalidateAll(keys)

    def invalidateAll(): Unit = underlying.synchronous().invalidateAll()

    def cleanUp(): Unit = underlying.synchronous().cleanUp()

    def getIfPresentSync(key: K): Option[V] = underlying.synchronous().getIfPresent(key)

    private val concurrentMap: scala.collection.concurrent.Map[K, CompletableFuture[V]] =
      underlying.underlying.asMap().asScala

    private def toFUSValue(v: CompletableFuture[V]): FutureUnlessShutdown[V] =
      tunnel.exit(
        FutureUtil.unwrapCompletionException(
          FutureConverters.asScala(v)
        )
      )

    private def toCompletableFutureValue(v: FutureUnlessShutdown[V]): CompletableFuture[V] =
      FutureConverters
        .asJava(
          tunnel.enter(v, context)
        )
        .toCompletableFuture

    def compute(key: K)(
        remappingFunction: Option[FutureUnlessShutdown[V]] => Option[FutureUnlessShutdown[V]]
    ): Option[FutureUnlessShutdown[V]] =
      concurrentMap
        .updateWith(key)(
          _.map(toFUSValue).pipe(remappingFunction).map(toCompletableFutureValue)
        )
        .map(toFUSValue)
  }

  class TunnelledAsyncCacheWithAuxCache[K, V, K2](
      val cache: Scaffeine[Any, Any],
      getAuxKeyO: V => Option[K2],
      tracedLogger: TracedLogger,
      context: String,
      sizeMetric: Gauge[Int],
      val loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext)
      extends TunnelledAsyncCache[K, V]
      with NamedLogging {

    private val auxCache: TrieMap[K2, K] = TrieMap.empty[K2, K]

    private def putAuxCache(key: K2, value: K): Unit = {
      auxCache.put(key, value).discard
      updateMetrics()
    }

    private def removeAuxCache(key: K2): Unit = {
      auxCache.remove(key).discard
      updateMetrics()
    }

    private val mainCache = ScaffeineCache.buildMappedAsync[K, V](
      cache = cache,
      evictionListener = Some { (_, v, _) =>
        getAuxKeyO(v).foreach(p => removeAuxCache(p))
      },
    )(tracedLogger, context)

    private def updateMetrics(): Unit =
      sizeMetric.updateValue(auxCache.size)

    def getFuture(
        key: K,
        mappingFunction: K => FutureUnlessShutdown[V],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[V] =
      // first check the main cache without compute to avoid unnecessary synchronization
      getIfPresentSync(key) match {
        case Some(value) => FutureUnlessShutdown.pure(value)
        case None =>
          // oncompleteGuard is used to determine whether we have just created a new entry in the cache without nesting
          // compute() calls which could lead to deadlocks for single-threaded executors
          @SuppressWarnings(Array("org.wartremover.warts.Var"))
          var oncompleteGuard = false
          val result =
            mainCache
              .compute(key) {
                case Some(f) => Some(f)
                // if the entry is not present, we set the oncomplete guard to true so that we know to update
                // the auxiliary cache when the future completes
                case None =>
                  oncompleteGuard = true
                  Some(mappingFunction(key))
              }
              .getOrElse(
                checked(
                  ErrorUtil.invalidState("Cache returned None while it was just populated")
                )
              )
          if (oncompleteGuard) {
            result.onComplete(_.foreach(_.foreach { _ =>
              // very important to not work on the result of the future, this is just a signal to execute the
              // computation, and the decisions need to be based on the current cache value
              mainCache
                .compute(key) {
                  case None => None //  already evicted, the aux cache should not be extended
                  case old @ Some(currentF) =>
                    currentF.unwrap.value match {
                      case Some(completed) =>
                        completed.foreach(
                          _.foreach(v => getAuxKeyO(v).foreach(keyAux => putAuxCache(keyAux, key)))
                        )
                      // another entry has already overwritten and is not completed yet, the auxiliary cache will be
                      // updated when the future is completed
                      case None => ()
                    }
                    old
                }
                .discard
            }))
          }
          result
      }

    def invalidate(key: K): Unit =
      mainCache
        .compute(key) {
          case None => None //  already evicted, the auxiliary cache should not be changed
          case Some(completed) if completed.isCompleted =>
            completed.unwrap.value.foreach(
              _.foreach(_.foreach(v => getAuxKeyO(v).foreach(keyAux => removeAuxCache(keyAux))))
            )
            None
          // compute() is atomic, so if we observe a non-completed future here, we know that the onComplete's compute()
          // added by getFuture() will happen after this block. The entry will not be present (None will be returned)
          // and thus the aux cache will not be updated by the onComplete code.
          // This ensures that the auxiliary cache entry is not added in the case of an eviction racing with the
          // completion of the future previously inserted.
          case Some(_notCompleted) => None
        }
        .discard

    def invalidateAll(keys: Iterable[K]): Unit = keys.foreach(invalidate)

    def invalidateAll(): Unit = {
      auxCache.clear()
      updateMetrics()
      mainCache.invalidateAll()
    }

    def cleanUp(): Unit = mainCache.cleanUp()

    def put(key: K, value: V): Unit =
      mainCache
        .compute(key) { v =>
          v.foreach(currentF =>
            currentF.unwrap.value match {
              case Some(completed) =>
                completed.foreach(
                  _.foreach(oldValue =>
                    // remove old auxiliary key mapping since it will be invalid now if the aux key has changed
                    getAuxKeyO(oldValue).foreach(keyAuxOld => removeAuxCache(keyAuxOld))
                  )
                )
              // compute() is atomic so if we observe a non-completed future here, we know that the onComplete's
              // compute() added by getFuture() will happen after this block. So, the aux cache does not contain any
              // mapping changed by the getFuture() yet. Since in the getFuture()'s onComplete callback we check for the
              // current entry of the cache we know that the aux cache can only be identically updated.
              // This ensures that the auxiliary cache entry does not contain an outdated mapping in the case of an
              // addition with put() racing with the completion of the future previously inserted.
              case None => ()
            }
          )
          getAuxKeyO(value).foreach(putAuxCache(_, key).discard)
          Some(FutureUnlessShutdown.pure(value))
        }
        .discard

    def getIfPresentSync(key: K): Option[V] = mainCache.getIfPresentSync(key: K)

    def getIfPresentAuxKey(keyAux: K2): Option[K] = auxCache.get(keyAux)
  }

  class TunnelledAsyncLoadingCache[F[_], K, V] private[ScaffeineCache] (
      underlying: AsyncLoadingCache[K, V],
      tracedLogger: TracedLogger,
      tunnel: EffectTunnel[F, Future],
      context: String,
  ) {
    implicit private[this] val ec: ExecutionContext = DirectExecutionContext(tracedLogger)

    /** @see
      *   com.github.blemale.scaffeine.AsyncLoadingCache.get
      */
    def get(key: K): F[V] = tunnel.exit(
      // The Caffeine cache's get method wraps exceptions from the loader in a `CompletionException`.
      // We should strip it here.
      FutureUtil.unwrapCompletionException(underlying.get(key))
    )

    /** @see
      *   com.github.blemale.scaffeine.AsyncLoadingCache.getAll
      */
    def getAll(keys: Iterable[K]): F[Map[K, V]] =
      tunnel.exit(
        // The Caffeine cache's getAll method wraps exceptions from the loader in a `CompletionException`.
        // We should strip it here.
        FutureUtil.unwrapCompletionException(underlying.getAll(keys))
      )

    /** @see
      *   com.github.blemale.scaffeine.AsyncLoadingCache.put
      */
    def put(key: K, value: V): Unit =
      underlying.put(key, Future.successful(value))

    /** Remove those mappings for which the predicate `filter` returns true */
    def clear(filter: (K, V) => Boolean): Unit =
      underlying.synchronous().asMap().filterInPlace((t, v) => !filter(t, v)).discard

    /** @see com.github.blemale.scaffeine.Cache.invalidate */
    def invalidate(key: K): Unit = underlying.synchronous().invalidate(key)

    /** @see com.github.blemale.scaffeine.Cache.invalidateAll */
    def invalidateAll(): Unit = underlying.synchronous().invalidateAll()

    /** @see com.github.blemale.scaffeine.Cache.cleanUp */
    def cleanUp(): Unit = underlying.synchronous().cleanUp()

    /** @see com.github.benmanes.caffeine.cache.AsyncCache.asMap */
    // Note: We cannot use compute to remove keys conditionally on the previous value because we'd have to distribute the Option out of the Future.
    def compute(key: K, remapper: (K, Option[V]) => F[V])(implicit
        F: FlatMap[F]
    ): F[V] = {
      def tunnelledRemapper: BiFunction[K, CompletableFuture[V], CompletableFuture[V]] =
        new BiFunction[K, CompletableFuture[V], CompletableFuture[V]] {
          override def apply(k: K, vF: CompletableFuture[V]): CompletableFuture[V] = {
            val remapped = Option(vF) match {
              case None => remapper(k, None)
              case Some(oldValueTunnelled) =>
                val oldValueF =
                  tunnel.exit(
                    FutureUtil.unwrapCompletionException(
                      FutureConverters.asScala(oldValueTunnelled)
                    )
                  )
                oldValueF.flatMap(oldValue => remapper(k, Some(oldValue)))
            }
            val remappedTunnel = tunnel.enter(remapped, context)
            FutureConverters.asJava(remappedTunnel).toCompletableFuture
          }
        }
      val remappedValueF = underlying.underlying.asMap().compute(key, tunnelledRemapper)
      tunnel.exit(FutureUtil.unwrapCompletionException(FutureConverters.asScala(remappedValueF)))
    }

    override def toString = s"TunnelledAsyncLoadingCache($underlying)"
  }

  class TracedAsyncLoadingCache[F[_], K, V] private[ScaffeineCache] (
      underlying: ScaffeineCache.TunnelledAsyncLoadingCache[F, Traced[K], V]
  )(implicit F: Functor[F]) {

    /** @see
      *   com.github.blemale.scaffeine.AsyncLoadingCache.get
      */
    def get(key: K)(implicit traceContext: TraceContext): F[V] =
      underlying.get(Traced(key))

    /** @see
      *   com.github.blemale.scaffeine.AsyncLoadingCache.getAll
      */
    def getAll(
        keys: Iterable[K]
    )(implicit traceContext: TraceContext): F[Map[K, V]] =
      F.map(underlying.getAll(keys.map(Traced(_))))(_.map { case (tracedKey, value) =>
        tracedKey.unwrap -> value
      })

    /** @see
      *   com.github.blemale.scaffeine.AsyncLoadingCache.put
      */
    def put(key: K, value: V)(implicit traceContext: TraceContext): Unit =
      underlying.put(Traced(key), value)

    /** Remove those mappings for which the predicate `filter` returns true */
    def clear(filter: (K, V) => Boolean): Unit =
      underlying.clear((t, v) => !filter(t.unwrap, v))

    def invalidate(key: K): Unit =
      // No need for a trace context here as we're simply invalidating an entry
      underlying.invalidate(Traced(key)(TraceContext.empty))

    def invalidateAll(): Unit = underlying.invalidateAll()

    def cleanUp(): Unit = underlying.cleanUp()

    // We intentionally do not pass the trace context found in the cache to the remapper
    // so that the remapper by default takes the trace context of the caller of compute.
    def compute(key: K, remapper: (K, Option[V]) => F[V])(implicit
        traceContext: TraceContext,
        F: FlatMap[F],
    ): F[V] =
      underlying.compute(
        Traced(key),
        (tracedKey, maybeValue) => remapper(tracedKey.unwrap, maybeValue),
      )

    override def toString: String = underlying.toString
  }
}
