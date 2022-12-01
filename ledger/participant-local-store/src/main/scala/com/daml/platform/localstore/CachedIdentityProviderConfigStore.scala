// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.caching.CaffeineCache
import com.daml.caching.CaffeineCache.FutureAsyncCacheLoader
import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, IdentityProviderConfigUpdate}
import com.github.benmanes.caffeine.{cache => caffeine}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.DurationConverters._
import scala.util.{Success, Try}

class CachedIdentityProviderConfigStore(
    delegate: IdentityProviderConfigStore,
    cacheExpiryAfterWrite: FiniteDuration,
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
        .expireAfterWrite(cacheExpiryAfterWrite.toJava)
        .maximumSize(maximumCacheSize.toLong)
        .buildAsync(
          new FutureAsyncCacheLoader[IdentityProviderId.Id, Result[IdentityProviderConfig]](
            delegate.getIdentityProviderConfig
          )
        ),
      metrics.daml.identityProviderConfigStore.cacheById,
    )

  private val idpByIssuer: CaffeineCache.AsyncLoadingCaffeineCache[
    String,
    Result[IdentityProviderConfig],
  ] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(cacheExpiryAfterWrite.toJava)
        .maximumSize(maximumCacheSize.toLong)
        .buildAsync(
          new FutureAsyncCacheLoader[String, Result[IdentityProviderConfig]](issuer =>
            delegate.getIdentityProviderConfig(issuer)
          )
        ),
      metrics.daml.identityProviderConfigStore.cacheByIssuer,
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
  ): Future[Result[Seq[IdentityProviderConfig]]] = delegate.listIdentityProviderConfigs()

  override def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] = delegate
    .updateIdentityProviderConfig(update)
    .andThen(invalidateOnSuccess(update.identityProviderId))

  override def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] =
    idpByIssuer.get(issuer)

  override def identityProviderConfigExists(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Boolean] =
    idpCache.get(id).map {
      case Right(_) => true
      case _ => false
    }

  private def invalidateOnSuccess(
      id: IdentityProviderId.Id
  ): PartialFunction[Try[Result[Any]], Unit] = { case Success(Right(_)) =>
    idpCache.invalidate(id)
    idpByIssuer.invalidateAll()
  }
}
