// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.caching.CaffeineCache
import com.digitalasset.canton.caching.CaffeineCache.FutureAsyncCacheLoader
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.digitalasset.canton.ledger.localstore.api.{
  IdentityProviderConfigStore,
  IdentityProviderConfigUpdate,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.github.benmanes.caffeine.cache as caffeine

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.DurationConverters.*
import scala.util.{Success, Try}

import IdentityProviderConfigStore.Result

class CachedIdentityProviderConfigStore(
    delegate: IdentityProviderConfigStore,
    cacheExpiryAfterWrite: FiniteDuration,
    maximumCacheSize: Int,
    metrics: Metrics,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext, loggingContext: LoggingContextWithTrace)
    extends IdentityProviderConfigStore
    with NamedLogging {

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
      metrics.daml.identityProviderConfigStore.idpConfigCache,
    )

  override def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] =
    delegate
      .createIdentityProviderConfig(identityProviderConfig)
      .andThen(invalidateByIssuerOnSuccess(identityProviderConfig.issuer))

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] = delegate.getIdentityProviderConfig(id)

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Unit]] =
    delegate.deleteIdentityProviderConfig(id).andThen(invalidateAllEntriesOnSuccess())

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Seq[IdentityProviderConfig]]] = delegate.listIdentityProviderConfigs()

  override def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] = delegate
    .updateIdentityProviderConfig(update)
    .andThen(invalidateAllEntriesOnSuccess())

  override def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] =
    idpByIssuer.get(issuer)

  override def identityProviderConfigExists(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
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
