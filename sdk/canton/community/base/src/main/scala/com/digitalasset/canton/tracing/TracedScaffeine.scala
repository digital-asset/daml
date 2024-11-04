// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import cats.~>
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.util.FutureUtil
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}

import scala.concurrent.{ExecutionContext, Future}

object TracedScaffeine {

  def buildTracedAsync[F[_], K, V](
      cache: Scaffeine[Any, Any],
      loader: TraceContext => K => F[V],
      allLoader: Option[TraceContext => Iterable[K] => F[Map[K, V]]] = None,
  )(
      tracedLogger: TracedLogger
  )(implicit tunnel: EffectTunnel[F, Future]): TracedAsyncLoadingCache[F, K, V] =
    new TracedAsyncLoadingCache[F, K, V](
      cache.buildAsyncFuture[Traced[K], V](
        loader = tracedKey => tunnel.enter(tracedKey.withTraceContext(loader)),
        allLoader = allLoader.map(tracedAllLoader(tracedLogger, _)(tunnel.enter)),
      ),
      tracedLogger,
      tunnel.exitK,
    )

  private def tracedAllLoader[F[_], K, V](
      tracedLogger: TracedLogger,
      allLoader: TraceContext => Iterable[K] => F[Map[K, V]],
  )(
      toFuture: F[Map[K, V]] => Future[Map[K, V]]
  ): Iterable[Traced[K]] => Future[Map[Traced[K], V]] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(tracedLogger)
    tracedKeys => {
      val traceContext = TraceContext.ofBatch(tracedKeys)(tracedLogger)
      val keys = tracedKeys.map(_.unwrap)
      toFuture(allLoader(traceContext)(keys))
        .map(_.map { case (key, value) => Traced(key)(traceContext) -> value })
    }
  }
}

class TracedAsyncLoadingCache[F[_], K, V] private[tracing] (
    underlying: AsyncLoadingCache[Traced[K], V],
    tracedLogger: TracedLogger,
    postProcess: Future ~> F,
) {
  implicit private[this] val ec: ExecutionContext = DirectExecutionContext(tracedLogger)

  /** @see com.github.blemale.scaffeine.AsyncLoadingCache.get
    */
  def get(key: K)(implicit traceContext: TraceContext): F[V] =
    postProcess(
      // The Caffeine cache's getAll method wraps exceptions from the loader in a `CompletionException`.
      // We should strip it here.
      FutureUtil.unwrapCompletionException(underlying.get(Traced(key)(traceContext)))
    )

  /** @see com.github.blemale.scaffeine.AsyncLoadingCache.getAll
    */
  def getAll(keys: Iterable[K])(implicit traceContext: TraceContext): F[Map[K, V]] =
    postProcess(
      // The Caffeine cache's getAll method wraps exceptions from the loader in a `CompletionException`.
      // We should strip it here.
      FutureUtil.unwrapCompletionException(
        underlying
          .getAll(keys.map(Traced(_)(traceContext)))
          .map(_.map { case (tracedKey, value) => tracedKey.unwrap -> value })(ec)
      )
    )

  /** Remove those mappings for which the predicate `filter` returns true */
  def clear(filter: (K, V) => Boolean): Unit =
    underlying.synchronous().asMap().filterInPlace((t, v) => !filter(t.unwrap, v)).discard

  override def toString = s"TracedAsyncLoadingCache($underlying)"
}
