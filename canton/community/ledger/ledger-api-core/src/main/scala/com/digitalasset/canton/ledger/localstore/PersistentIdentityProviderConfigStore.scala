// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.digitalasset.canton.ledger.localstore.CachedIdentityProviderConfigStore
import com.digitalasset.canton.ledger.localstore.Ops.*
import com.digitalasset.canton.ledger.localstore.api.IdentityProviderConfigStore.*
import com.digitalasset.canton.ledger.localstore.api.{
  IdentityProviderConfigStore,
  IdentityProviderConfigUpdate,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.tracing.TraceContext

import java.sql.Connection
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class PersistentIdentityProviderConfigStore(
    dbSupport: DbSupport,
    metrics: Metrics,
    maxIdentityProviders: Int,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends IdentityProviderConfigStore
    with NamedLogging {

  private val backend = dbSupport.storageBackendFactory.createIdentityProviderConfigStorageBackend
  private val dbDispatcher = dbSupport.dbDispatcher

  override def createIdentityProviderConfig(identityProviderConfig: domain.IdentityProviderConfig)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Result[domain.IdentityProviderConfig]] =
    inTransaction(_.createIdpConfig) { implicit connection =>
      val id = identityProviderConfig.identityProviderId
      for {
        _ <- idpConfigDoesNotExist(id)
        _ <- idpConfigByIssuerDoesNotExist(
          Some(identityProviderConfig.issuer),
          identityProviderConfig.identityProviderId,
        )
        _ = backend.createIdentityProviderConfig(identityProviderConfig)(connection)
        _ <- tooManyIdentityProviderConfigs()(connection)
        domainConfig <- backend
          .getIdentityProviderConfig(id)(connection)
          .toRight(IdentityProviderConfigNotFound(id))
      } yield domainConfig
    }.map(tapSuccess { cfg =>
      logger.info(
        s"Created new identity provider configuration: $cfg"
      )
    })

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[domain.IdentityProviderConfig]] =
    inTransaction(_.getIdpConfig) { implicit connection =>
      backend
        .getIdentityProviderConfig(id)(connection)
        .toRight(IdentityProviderConfigNotFound(id))
    }

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Unit]] =
    inTransaction(_.deleteIdpConfig) { implicit connection =>
      if (!backend.deleteIdentityProviderConfig(id)(connection)) {
        Left(IdentityProviderConfigNotFound(id))
      } else {
        Right(())
      }
    }.map(tapSuccess { _ =>
      logger.info(
        s"Deleted identity provider configuration with id $id"
      )
    })

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Seq[domain.IdentityProviderConfig]]] = {
    inTransaction(_.listIdpConfigs) { implicit connection =>
      Right(backend.listIdentityProviderConfigs()(connection))
    }
  }

  override def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[domain.IdentityProviderConfig]] = {
    inTransaction(_.updateIdpConfig) { implicit connection =>
      val id = update.identityProviderId
      for {
        _ <- idpConfigExists(id)
        _ <- idpConfigByIssuerDoesNotExist(update.issuerUpdate, update.identityProviderId)
        _ <- updateIssuer(update)(connection)
        _ <- updateJwksUrl(update)(connection)
        _ <- updateAudience(update)(connection)
        _ <- updateIsDeactivated(update)(connection)
        identityProviderConfig <- backend
          .getIdentityProviderConfig(id)(connection)
          .toRight(IdentityProviderConfigNotFound(id))
      } yield identityProviderConfig
    }.map(tapSuccess { _ =>
      logger.info(
        s"Updated identity provider configuration with id ${update.identityProviderId}"
      )
    })
  }

  override def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]] = inTransaction(_.getIdpConfig) { implicit connection =>
    for {
      identityProviderConfig <- backend
        .getIdentityProviderConfigByIssuer(issuer)(connection)
        .toRight(IdentityProviderConfigByIssuerNotFound(issuer))
    } yield identityProviderConfig
  }

  def identityProviderConfigExists(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Boolean] = {
    dbDispatcher.executeSql(metrics.daml.identityProviderConfigStore.getIdpConfig) { connection =>
      backend.idpConfigByIdExists(id)(connection)
    }
  }

  private def updateIssuer(
      update: IdentityProviderConfigUpdate
  )(connection: Connection): Result[Unit] = {
    val execute =
      update.issuerUpdate.forall(backend.updateIssuer(update.identityProviderId, _)(connection))
    Either.cond(execute, (), IdentityProviderConfigNotFound(update.identityProviderId))
  }
  private def updateJwksUrl(
      update: IdentityProviderConfigUpdate
  )(connection: Connection): Result[Unit] = {
    val execute = update.jwksUrlUpdate.forall(
      backend.updateJwksUrl(update.identityProviderId, _)(connection)
    )
    Either.cond(execute, (), IdentityProviderConfigNotFound(update.identityProviderId))
  }

  private def updateAudience(
      update: IdentityProviderConfigUpdate
  )(connection: Connection): Result[Unit] = {
    val execute = update.audienceUpdate.forall(
      backend.updateAudience(update.identityProviderId, _)(connection)
    )
    Either.cond(execute, (), IdentityProviderConfigNotFound(update.identityProviderId))
  }

  private def updateIsDeactivated(
      update: IdentityProviderConfigUpdate
  )(connection: Connection): Result[Unit] = {
    val execute = update.isDeactivatedUpdate.forall(
      backend.updateIsDeactivated(update.identityProviderId, _)(connection)
    )
    Either.cond(execute, (), IdentityProviderConfigNotFound(update.identityProviderId))
  }

  private def tooManyIdentityProviderConfigs()(
      connection: Connection
  ): Result[Unit] =
    Either.cond(
      backend.countIdentityProviderConfigs()(connection) <= maxIdentityProviders,
      (),
      TooManyIdentityProviderConfigs(),
    )

  private def idpConfigExists(
      id: IdentityProviderId.Id
  )(implicit connection: Connection): Result[Unit] = Either.cond(
    backend.idpConfigByIdExists(id)(connection),
    (),
    IdentityProviderConfigNotFound(id),
  )

  private def idpConfigDoesNotExist(
      id: IdentityProviderId.Id
  )(implicit connection: Connection): Result[Unit] = Either.cond(
    !backend.idpConfigByIdExists(id)(connection),
    (),
    IdentityProviderConfigExists(id),
  )

  private def idpConfigByIssuerDoesNotExist(
      issuer: Option[String],
      id: IdentityProviderId.Id,
  )(implicit connection: Connection): Result[Unit] = issuer match {
    case Some(value) =>
      Either.cond(
        !backend.identityProviderConfigByIssuerExists(id, value)(connection),
        (),
        IdentityProviderConfigWithIssuerExists(value),
      )
    case None => Right(())
  }

  private def inTransaction[T](
      dbMetric: metrics.daml.identityProviderConfigStore.type => DatabaseMetrics
  )(
      thunk: Connection => Result[T]
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[T]] =
    dbDispatcher
      .executeSqlEither(dbMetric(metrics.daml.identityProviderConfigStore))(thunk)

  private def tapSuccess[T](f: T => Unit)(r: Result[T]): Result[T] = {
    r.foreach(f)
    r
  }

}

object PersistentIdentityProviderConfigStore {
  def cached(
      dbSupport: DbSupport,
      metrics: Metrics,
      cacheExpiryAfterWrite: FiniteDuration,
      maxIdentityProviders: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ) = new CachedIdentityProviderConfigStore(
    delegate = new PersistentIdentityProviderConfigStore(
      dbSupport,
      metrics,
      maxIdentityProviders,
      loggerFactory,
    ),
    cacheExpiryAfterWrite = cacheExpiryAfterWrite,
    maximumCacheSize = maxIdentityProviders,
    metrics = metrics,
    loggerFactory,
  )(executionContext, LoggingContextWithTrace(loggerFactory))
}
