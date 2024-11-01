// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import cats.~>
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.util.FutureUtil
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}

import scala.concurrent.{ExecutionContext, Future}

object TracedScaffeine {
  def buildTracedAsyncFuture[K, V](
      cache: Scaffeine[Any, Any],
      loader: TraceContext => K => Future[V],
      allLoader: Option[TraceContext => Iterable[K] => Future[Map[K, V]]] = None,
  )(
      tracedLogger: TracedLogger
  )(implicit ec: ExecutionContext): TracedAsyncLoadingCache[Future, K, V] =
    new TracedAsyncLoadingCache[Future, K, V](
      cache.buildAsyncFuture[Traced[K], V](
        loader = tracedKey => loader(tracedKey.traceContext)(tracedKey.unwrap),
        allLoader = allLoader.map { tracedFunction => (tracedKeys: Iterable[Traced[K]]) =>
          {
            val traceContext = tracedKeys.headOption
              .map(_.traceContext)
              .getOrElse(TraceContext.empty)
            val keys = tracedKeys.map(_.unwrap)
            tracedFunction(traceContext)(keys)
              .map(_.map { case (key, value) => Traced(key)(traceContext) -> value })
          }
        },
      ),
      tracedLogger,
      // The Caffeine cache's getAll method wraps exceptions from the loader in a `CompletionException`.
      // We should strip it here.
      FutureUtil.unwrapCompletionExceptionK,
    )

  def buildTracedAsyncFutureUS[K, V](
      cache: Scaffeine[Any, Any],
      loader: TraceContext => K => FutureUnlessShutdown[V],
      allLoader: Option[TraceContext => Iterable[K] => FutureUnlessShutdown[Map[K, V]]] = None,
  )(
      tracedLogger: TracedLogger
  )(implicit ec: ExecutionContext): TracedAsyncLoadingCache[FutureUnlessShutdown, K, V] =
    new TracedAsyncLoadingCache[FutureUnlessShutdown, K, V](
      cache.buildAsyncFuture[Traced[K], V](
        loader = _.withTraceContext(loader)
          .failOnShutdownToAbortException("TracedAsyncLoadingCache-loader"),
        allLoader = allLoader.map { tracedFunction => (tracedKeys: Iterable[Traced[K]]) =>
          {
            val traceContext = TraceContext.ofBatch(tracedKeys)(tracedLogger)
            val keys = tracedKeys.map(_.unwrap)
            tracedFunction(traceContext)(keys)
              .map(_.map { case (key, value) => Traced(key)(traceContext) -> value })
          }.failOnShutdownToAbortException("TracedAsyncLoadingCache-allLoader")
        },
      ),
      tracedLogger,
      // The Caffeine cache's getAll method wraps exceptions from the loader in a `CompletionException`.
      // We should strip it here.
      FutureUnlessShutdown.recoverFromAbortExceptionK.compose(FutureUtil.unwrapCompletionExceptionK),
    )

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
    postProcess(underlying.get(Traced(key)(traceContext)))

  /** @see com.github.blemale.scaffeine.AsyncLoadingCache.getAll
    */
  def getAll(keys: Iterable[K])(implicit traceContext: TraceContext): F[Map[K, V]] =
    postProcess(
      underlying
        .getAll(keys.map(Traced(_)(traceContext)))
        .map(_.map { case (tracedKey, value) => tracedKey.unwrap -> value })(ec)
    )

  /** Remove those mappings for which the predicate `filter` returns true */
  def clear(filter: (K, V) => Boolean): Unit =
    underlying.synchronous().asMap().filterInPlace((t, v) => !filter(t.unwrap, v)).discard

  override def toString = s"TracedAsyncLoadingCache($underlying)"
}
