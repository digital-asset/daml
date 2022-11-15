// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.caching.CaffeineCache
import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref.IdentityProviderId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.localstore.api.IdentityProviderConfigStore
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result
import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import com.github.benmanes.caffeine.{cache => caffeine}

import java.time.Duration
import java.util.concurrent.{CompletableFuture, Executor}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CachedIdentityProviderConfigStore(
    delegate: IdentityProviderConfigStore,
    expiryAfterWriteInSeconds: Int,
    maximumCacheSize: Int,
    metrics: Metrics,
)(implicit val executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends IdentityProviderConfigStore {

  private val cache: CaffeineCache.AsyncLoadingCaffeineCache[
    IdentityProviderId.Id,
    Result[IdentityProviderConfig],
  ] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        .maximumSize(maximumCacheSize.toLong)
        .buildAsync(
          new AsyncCacheLoader[IdentityProviderId.Id, Result[IdentityProviderConfig]] {
            override def asyncLoad(
                key: IdentityProviderId.Id,
                executor: Executor,
            ): CompletableFuture[Result[IdentityProviderConfig]] = {
              val cf = new CompletableFuture[Result[IdentityProviderConfig]]
              delegate.getIdentityProviderConfig(key).onComplete {
                case Success(value) => cf.complete(value)
                case Failure(e) => cf.completeExceptionally(e)
              }
              cf
            }
          }
        ),
      metrics.daml.identityProviderConfigStore.cache,
    )

  private val listCache: CaffeineCache.AsyncLoadingCaffeineCache[
    CaffeineCache.type,
    Result[Seq[IdentityProviderConfig]],
  ] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        .maximumSize(1)
        .buildAsync(
          new AsyncCacheLoader[CaffeineCache.type, Result[Seq[IdentityProviderConfig]]] {
            override def asyncLoad(
                key: CaffeineCache.type,
                executor: Executor,
            ): CompletableFuture[Result[Seq[IdentityProviderConfig]]] = {
              val cf = new CompletableFuture[Result[Seq[IdentityProviderConfig]]]
              delegate.listIdentityProviderConfigs().onComplete {
                case Success(value) => cf.complete(value)
                case Failure(e) => cf.completeExceptionally(e)
              }
              cf
            }
          }
        ),
      metrics.daml.identityProviderConfigStore.cache,
    )

  override def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] =
    delegate
      .createIdentityProviderConfig(identityProviderConfig)
      .andThen(invalidateOnSuccess(identityProviderConfig.identityProviderId))

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] = cache.get(id)

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]] =
    delegate.deleteIdentityProviderConfig(id).andThen(invalidateOnSuccess(id))

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContext
  ): Future[Result[Seq[IdentityProviderConfig]]] = listCache.get(CaffeineCache)

  private def invalidateOnSuccess(
      id: IdentityProviderId.Id
  ): PartialFunction[Try[Result[Any]], Unit] = { case Success(Right(_)) =>
    cache.invalidate(id)
    listCache.invalidate(CaffeineCache)
  }
}
