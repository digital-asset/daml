// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import cats.syntax.flatMap.*
import cats.{FlatMap, Functor}
import com.daml.metrics.CacheMetrics
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureUtil
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}

import java.util.concurrent.CompletableFuture
import java.util.function.BiFunction
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.javaapi.FutureConverters

object ScaffeineCache {

  def buildAsync[F[_], K, V](
      cache: Scaffeine[Any, Any],
      loader: K => F[V],
      allLoader: Option[Iterable[K] => F[Map[K, V]]] = None,
      metrics: Option[CacheMetrics] = None,
  )(
      tracedLogger: TracedLogger
  )(implicit tunnel: EffectTunnel[F, Future]): TunnelledAsyncLoadingCache[F, K, V] = {
    val asyncLoadingCache = cache.buildAsyncFuture[K, V](
      loader = key => tunnel.enter(loader(key)),
      allLoader = allLoader.map(loader => keys => tunnel.enter(loader(keys))),
    )
    metrics.foreach(CaffeineCache.installMetrics(_, asyncLoadingCache.underlying.synchronous()))
    new TunnelledAsyncLoadingCache[F, K, V](asyncLoadingCache, tracedLogger, tunnel)
  }

  def buildTracedAsync[F[_], K, V](
      cache: Scaffeine[Any, Any],
      loader: TraceContext => K => F[V],
      allLoader: Option[TraceContext => Iterable[K] => F[Map[K, V]]] = None,
      metrics: Option[CacheMetrics] = None,
  )(
      tracedLogger: TracedLogger
  )(implicit tunnel: EffectTunnel[F, Future], F: Functor[F]): TracedAsyncLoadingCache[F, K, V] = {
    val asyncCache = ScaffeineCache.buildAsync[F, Traced[K], V](
      cache,
      loader = _.withTraceContext(loader),
      allLoader = allLoader.map(tracedAllLoader(tracedLogger, _)),
      metrics,
    )(tracedLogger)
    new TracedAsyncLoadingCache[F, K, V](asyncCache)
  }

  private def tracedAllLoader[F[_], K, V](
      tracedLogger: TracedLogger,
      allLoader: TraceContext => Iterable[K] => F[Map[K, V]],
  )(implicit F: Functor[F]): Iterable[Traced[K]] => F[Map[Traced[K], V]] = { tracedKeys =>
    val traceContext = TraceContext.ofBatch(tracedKeys)(tracedLogger)
    val keys = tracedKeys.map(_.unwrap)
    F.map(allLoader(traceContext)(keys))(_.map { case (key, value) =>
      Traced(key)(traceContext) -> value
    })
  }

  class TunnelledAsyncLoadingCache[F[_], K, V] private[ScaffeineCache] (
      underlying: AsyncLoadingCache[K, V],
      tracedLogger: TracedLogger,
      tunnel: EffectTunnel[F, Future],
  ) {
    implicit private[this] val ec: ExecutionContext = DirectExecutionContext(tracedLogger)

    /** @see com.github.blemale.scaffeine.AsyncLoadingCache.get
      */
    def get(key: K): F[V] = tunnel.exit(
      // The Caffeine cache's get method wraps exceptions from the loader in a `CompletionException`.
      // We should strip it here.
      FutureUtil.unwrapCompletionException(underlying.get(key))
    )

    /** @see com.github.blemale.scaffeine.AsyncLoadingCache.getAll
      */
    def getAll(keys: Iterable[K]): F[Map[K, V]] =
      tunnel.exit(
        // The Caffeine cache's getAll method wraps exceptions from the loader in a `CompletionException`.
        // We should strip it here.
        FutureUtil.unwrapCompletionException(underlying.getAll(keys))
      )

    /** @see com.github.blemale.scaffeine.AsyncLoadingCache.put
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
            val remappedTunnel = tunnel.enter(remapped)
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

    /** @see com.github.blemale.scaffeine.AsyncLoadingCache.get
      */
    def get(key: K)(implicit traceContext: TraceContext): F[V] =
      underlying.get(Traced(key))

    /** @see com.github.blemale.scaffeine.AsyncLoadingCache.getAll
      */
    def getAll(
        keys: Iterable[K]
    )(implicit traceContext: TraceContext): F[Map[K, V]] =
      F.map(underlying.getAll(keys.map(Traced(_))))(_.map { case (tracedKey, value) =>
        tracedKey.unwrap -> value
      })

    /** @see com.github.blemale.scaffeine.AsyncLoadingCache.put
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
