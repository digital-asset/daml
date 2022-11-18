// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.caching.CaffeineCache
import com.daml.caching.CaffeineCache.FutureAsyncCacheLoader
import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref.IdentityProviderId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, IdentityProviderConfigUpdate}
import com.github.benmanes.caffeine.{cache => caffeine}

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class CachedIdentityProviderConfigStore(
    delegate: IdentityProviderConfigStore,
    expiryAfterWriteInSeconds: Int,
    maximumCacheSize: Int,
    metrics: Metrics,
)(implicit val executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends IdentityProviderConfigStore {

  private val idpCache: CaffeineCache.AsyncLoadingCaffeineCache[
    IdentityProviderId.Id,
    Result[IdentityProviderConfig],
  ] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        .maximumSize(maximumCacheSize.toLong)
        .buildAsync(
          new FutureAsyncCacheLoader[IdentityProviderId.Id, Result[IdentityProviderConfig]](
            delegate.getIdentityProviderConfig
          )
        ),
      metrics.daml.identityProviderConfigStore.cache,
    )

  private val idpListCache: CaffeineCache.AsyncLoadingCaffeineCache[
    CaffeineCache.type,
    Result[Seq[IdentityProviderConfig]],
  ] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        .maximumSize(1)
        .buildAsync(
          new FutureAsyncCacheLoader[CaffeineCache.type, Result[Seq[IdentityProviderConfig]]](_ =>
            delegate.listIdentityProviderConfigs()
          )
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
  ): Future[Result[IdentityProviderConfig]] = idpCache.get(id)

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]] =
    delegate.deleteIdentityProviderConfig(id).andThen(invalidateOnSuccess(id))

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContext
  ): Future[Result[Seq[IdentityProviderConfig]]] = idpListCache.get(CaffeineCache)

  override def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] = delegate
    .updateIdentityProviderConfig(update)
    .andThen(invalidateOnSuccess(update.identityProviderId))

  private def invalidateOnSuccess(
      id: IdentityProviderId.Id
  ): PartialFunction[Try[Result[Any]], Unit] = { case Success(Right(_)) =>
    idpCache.invalidate(id)
    idpListCache.invalidate(CaffeineCache)
  }
}
