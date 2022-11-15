// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain
import com.daml.lf.data.Ref.IdentityProviderId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.localstore.api.IdentityProviderConfigStore
import com.daml.platform.localstore.api.IdentityProviderConfigStore.{
  IdentityProviderConfigExists,
  IdentityProviderConfigWithIssuerExists,
  Result,
}
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
      withoutIDPConfig(identityProviderConfig.identityProviderId, identityProviderConfig.issuer) {
        backend.createIdentityProviderConfig(identityProviderConfig)(connection)
        identityProviderConfig
      }
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
      withIDPConfig(id)(identity)
    }
  }

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]] = {
    inTransaction(_.deleteIDPConfig) { implicit connection =>
      if (!backend.deleteIdentityProviderConfig(id)(connection)) {
        Left(IdentityProviderConfigStore.IdentityProviderConfigNotFound(id))
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
    inTransaction(_.deleteIDPConfig) { implicit connection =>
      Right(backend.listIdentityProviderConfigs()(connection))
    }
  }

  private def inTransaction[T](
      dbMetric: metrics.daml.identityProviderConfigStore.type => DatabaseMetrics
  )(thunk: Connection => Result[T])(implicit loggingContext: LoggingContext): Future[Result[T]] =
    dbDispatcher
      .executeSql(dbMetric(metrics.daml.identityProviderConfigStore))(thunk)

  private def withoutIDPConfig[T](
      id: IdentityProviderId.Id,
      issuer: String,
  )(t: => T)(implicit connection: Connection): Result[T] = {
    val idExists = backend.idpConfigByIdExists(id)(connection)
    val issuerExists = backend.idpConfigByIssuerExists(issuer)(connection)
    (idExists, issuerExists) match {
      case (true, _) =>
        Left(
          IdentityProviderConfigExists(identityProviderId = id)
        )
      case (_, true) =>
        Left(
          IdentityProviderConfigWithIssuerExists(issuer)
        )
      case (false, false) => Right(t)
    }
  }

  private def withIDPConfig[T](
      id: IdentityProviderId.Id
  )(
      f: domain.IdentityProviderConfig => T
  )(implicit connection: Connection): Result[T] = {
    backend.getIdentityProviderConfig(id)(connection) match {
      case Some(partyRecord) => Right(f(partyRecord))
      case None => Left(IdentityProviderConfigStore.IdentityProviderConfigNotFound(id))
    }
  }

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
