// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.localstore.api.IdentityProviderConfigStore.{
  IdentityProviderConfigExists,
  IdentityProviderConfigNotFound,
  IdentityProviderConfigWithIssuerExists,
  Result,
}
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, IdentityProviderConfigUpdate}
import com.daml.platform.store.DbSupport

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

class PersistentIdentityProviderConfigStore(
    dbSupport: DbSupport,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends IdentityProviderConfigStore {

  private val backend = dbSupport.storageBackendFactory.createIdentityProviderConfigStorageBackend
  private val dbDispatcher = dbSupport.dbDispatcher
  private val logger = ContextualizedLogger.get(getClass)

  override def createIdentityProviderConfig(identityProviderConfig: domain.IdentityProviderConfig)(
      implicit loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = {
    inTransaction(_.createIDPConfig) { implicit connection =>
      val id = identityProviderConfig.identityProviderId
      for {
        _ <- idpConfigDoesNotExist(id)
        _ <- idpConfigIssuerDoesNotExist(Some(identityProviderConfig.issuer))
        _ = backend.createIdentityProviderConfig(identityProviderConfig)(connection)
        domainConfig <- backend
          .getIdentityProviderConfig(id)(connection)
          .toRight(IdentityProviderConfigNotFound(id))
      } yield domainConfig
    }.map(tapSuccess { _ =>
      logger.info(
        s"Created new identity provider configuration: $identityProviderConfig"
      )
    })
  }

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = {
    inTransaction(_.getIDPConfig) { implicit connection =>
      backend
        .getIdentityProviderConfig(id)(connection)
        .toRight(IdentityProviderConfigNotFound(id))
    }
  }

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]] = {
    inTransaction(_.deleteIDPConfig) { implicit connection =>
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
  }

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContext
  ): Future[Result[Seq[domain.IdentityProviderConfig]]] = {
    inTransaction(_.listIDPConfigs) { implicit connection =>
      Right(backend.listIdentityProviderConfigs()(connection))
    }
  }

  override def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = {
    inTransaction(_.updateIDPConfig) { implicit connection =>
      val id = update.identityProviderId
      for {
        _ <- idpConfigExists(id)
        _ <- idpConfigIssuerDoesNotExist(update.issuerUpdate)
        _ = {
          update.issuerUpdate.foreach(
            backend.updateIssuer(update.identityProviderId, _)(connection)
          )
          update.jwksUrlUpdate.foreach(
            backend.updateJwksURL(update.identityProviderId, _)(connection)
          )
          update.isDeactivatedUpdate.foreach(
            backend.updateIsDeactivated(update.identityProviderId, _)(connection)
          )
        }
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

  private def idpConfigIssuerDoesNotExist(
      issuer: Option[String]
  )(implicit connection: Connection): Result[Unit] = issuer match {
    case Some(value) =>
      Either.cond(
        !backend.idpConfigByIssuerExists(value)(connection),
        (),
        IdentityProviderConfigWithIssuerExists(value),
      )
    case None => Right(())
  }

  private def inTransaction[T](
      dbMetric: metrics.daml.identityProviderConfigStore.type => DatabaseMetrics
  )(thunk: Connection => Result[T])(implicit loggingContext: LoggingContext): Future[Result[T]] =
    dbDispatcher
      .executeSql(dbMetric(metrics.daml.identityProviderConfigStore))(thunk)

  private def tapSuccess[T](f: T => Unit)(r: Result[T]): Result[T] = {
    r.foreach(f)
    r
  }

}

object PersistentIdentityProviderConfigStore {
  def cached(
      dbSupport: DbSupport,
      metrics: Metrics,
      expiryAfterWriteInSeconds: Int,
      maximumCacheSize: Int,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ) = new CachedIdentityProviderConfigStore(
    new PersistentIdentityProviderConfigStore(dbSupport, metrics),
    expiryAfterWriteInSeconds,
    maximumCacheSize,
    metrics,
  )
}
