// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.ledger.api.{IdentityProviderConfig, IdentityProviderId}
import com.digitalasset.canton.ledger.localstore.api.{
  IdentityProviderConfigStore,
  IdentityProviderConfigUpdate,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.github.blemale.scaffeine.Scaffeine

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import IdentityProviderConfigStore.Result

class CachedIdentityProviderConfigStore(
    delegate: IdentityProviderConfigStore,
    cacheExpiryAfterWrite: FiniteDuration,
    maximumCacheSize: Int,
    metrics: LedgerApiServerMetrics,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext, loggingContext: LoggingContextWithTrace)
    extends IdentityProviderConfigStore
    with NamedLogging {

  private val idpByIssuer
      : ScaffeineCache.TunnelledAsyncLoadingCache[Future, String, Result[IdentityProviderConfig]] =
    ScaffeineCache.buildAsync[Future, String, Result[IdentityProviderConfig]](
      Scaffeine()
        .expireAfterWrite(cacheExpiryAfterWrite)
        .maximumSize(maximumCacheSize.toLong),
      loader = issuer => delegate.getIdentityProviderConfig(issuer),
      metrics = Some(metrics.identityProviderConfigStore.idpConfigCache),
    )(logger, "idpByIssuer")

  override def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] =
    delegate
      .createIdentityProviderConfig(identityProviderConfig)
      .thereafter(invalidateByIssuerOnSuccess(identityProviderConfig.issuer))

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] = delegate.getIdentityProviderConfig(id)

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Unit]] =
    delegate.deleteIdentityProviderConfig(id).thereafter(invalidateAllEntriesOnSuccess())

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Seq[IdentityProviderConfig]]] = delegate.listIdentityProviderConfigs()

  override def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] = delegate
    .updateIdentityProviderConfig(update)
    .thereafter(invalidateAllEntriesOnSuccess())

  override def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] =
    idpByIssuer.get(issuer)

  override def identityProviderConfigExists(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Boolean] =
    delegate.identityProviderConfigExists(id)

  private def invalidateAllEntriesOnSuccess(): Try[Result[Any]] => Unit =
    _.foreach(_.foreach(_ => idpByIssuer.invalidateAll()))

  private def invalidateByIssuerOnSuccess(
      issuer: String
  ): Try[Result[Any]] => Unit =
    _.foreach(_.foreach(_ => idpByIssuer.invalidate(issuer)))
}
