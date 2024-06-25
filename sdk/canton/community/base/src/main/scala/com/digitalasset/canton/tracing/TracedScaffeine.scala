// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}

import scala.concurrent.{ExecutionContext, Future}

private[tracing] final case class TracedKey[K](key: K)(val traceContext: TraceContext)

object TracedScaffeine {
  def buildTracedAsyncFuture[K1, V1](
      cache: Scaffeine[Any, Any],
      loader: TraceContext => K1 => Future[V1],
      allLoader: Option[TraceContext => Iterable[K1] => Future[Map[K1, V1]]] = None,
  )(
      tracedLogger: TracedLogger
  )(implicit ec: ExecutionContext): TracedAsyncLoadingCache[K1, V1] = {

    new TracedAsyncLoadingCache[K1, V1](
      cache.buildAsyncFuture[TracedKey[K1], V1](
        loader = tracedKey => loader(tracedKey.traceContext)(tracedKey.key),
        allLoader = allLoader.map { tracedFunction => (tracedKeys: Iterable[TracedKey[K1]]) =>
          {
            val traceContext = tracedKeys.headOption
              .map(_.traceContext)
              .getOrElse(TraceContext.empty)
            val keys = tracedKeys.map(_.key)
            tracedFunction(traceContext)(keys)
              .map(_.map { case (key, value) => TracedKey(key)(traceContext) -> value })
          }
        },
      )
    )(tracedLogger)

  }

  def buildTracedAsyncFutureUS[K1, V1](
      cache: Scaffeine[Any, Any],
      loader: TraceContext => K1 => FutureUnlessShutdown[V1],
      allLoader: Option[TraceContext => Iterable[K1] => FutureUnlessShutdown[Map[K1, V1]]] = None,
  )(
      tracedLogger: TracedLogger
  )(implicit ec: ExecutionContext): TracedAsyncLoadingCache[K1, V1] = {

    new TracedAsyncLoadingCache[K1, V1](
      cache.buildAsyncFuture[TracedKey[K1], V1](
        loader = tracedKey =>
          loader(tracedKey.traceContext)(tracedKey.key)
            .failOnShutdownToAbortException("TracedAsyncLoadingCache-loader"),
        allLoader = allLoader.map { tracedFunction => (tracedKeys: Iterable[TracedKey[K1]]) =>
          {
            val traceContext = tracedKeys.headOption
              .map(_.traceContext)
              .getOrElse(TraceContext.empty)
            val keys = tracedKeys.map(_.key)
            tracedFunction(traceContext)(keys)
              .map(_.map { case (key, value) => TracedKey(key)(traceContext) -> value })
          }.failOnShutdownToAbortException("TracedAsyncLoadingCache-allLoader")
        },
      )
    )(tracedLogger)

  }
}

class TracedAsyncLoadingCache[K, V](
    underlying: AsyncLoadingCache[TracedKey[K], V]
)(tracedLogger: TracedLogger) {
  implicit private[this] val ec: ExecutionContext = DirectExecutionContext(tracedLogger)

  /* See com.github.blemale.scaffeine.AsyncLoadingCache.get
   * If shutting down the future returned will be failed with a AbortedDueToShutdownException
   */
  def get(key: K)(implicit traceContext: TraceContext): Future[V] =
    underlying.get(TracedKey(key)(traceContext))

  // Remove those mappings for which the predicate p returns true
  def clear(filter: (K, V) => Boolean): Unit = {
    discard(underlying.synchronous().asMap().filterInPlace((t, v) => !filter(t.key, v)))
  }

  def getUS(key: K)(implicit traceContext: TraceContext): FutureUnlessShutdown[V] =
    FutureUnlessShutdown.transformAbortedF(get(key))

  /* See [[com.github.blemale.scaffeine.AsyncLoadingCache.getAll]]
   *  If shutting down the future returned will be failed with a AbortedDueToShutdownException wrapped inside
   *  a java.util.concurrent.CompletionException
   */
  def getAll(keys: Iterable[K])(implicit traceContext: TraceContext): Future[Map[K, V]] =
    underlying
      .getAll(keys.map(TracedKey(_)(traceContext)))
      .map(_.map { case (tracedKey, value) => tracedKey.key -> value })(ec)

  def getAllUS(
      keys: Iterable[K]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[K, V]] = {
    FutureUnlessShutdown.transformAbortedF(getAll(keys))
  }

  override def toString = s"TracedAsyncLoadingCache($underlying)"
}
