// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      .andThen(invalidateByIssuerOnSuccess(identityProviderConfig.issuer))

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] = delegate.getIdentityProviderConfig(id)

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]] =
    delegate.deleteIdentityProviderConfig(id).andThen(invalidateAllEntriesOnSuccess())

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContext
  ): Future[Result[Seq[IdentityProviderConfig]]] = delegate.listIdentityProviderConfigs()

  override def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] = delegate
    .updateIdentityProviderConfig(update)
    .andThen(invalidateAllEntriesOnSuccess())

  override def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] =
    idpByIssuer.get(issuer)

  override def identityProviderConfigExists(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Boolean] =
    delegate.identityProviderConfigExists(id)

  private def invalidateAllEntriesOnSuccess(): PartialFunction[Try[Result[Any]], Unit] = {
    case Success(Right(_)) =>
      idpByIssuer.invalidateAll()
  }

  private def invalidateByIssuerOnSuccess(
      issuer: String
  ): PartialFunction[Try[Result[Any]], Unit] = { case Success(Right(_)) =>
    idpByIssuer.invalidate(issuer)
  }
}
