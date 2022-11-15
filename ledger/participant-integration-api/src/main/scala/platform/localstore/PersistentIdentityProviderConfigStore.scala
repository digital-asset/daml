// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain
import com.daml.lf.data.Ref.IdentityProviderId
import com.daml.logging.LoggingContext
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.localstore.api.IdentityProviderConfigStore
import com.daml.platform.localstore.api.IdentityProviderConfigStore.{
  IdentityProviderConfigExists,
  Result,
}
import com.daml.platform.store.DbSupport

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

class PersistentIdentityProviderConfigStore(
    dbSupport: DbSupport,
    metrics: Metrics,
) extends IdentityProviderConfigStore {

  private val backend = dbSupport.storageBackendFactory.createIdentityProviderConfigStorageBackend
  private val dbDispatcher = dbSupport.dbDispatcher

  override def createIdentityProviderConfig(identityProviderConfig: domain.IdentityProviderConfig)(
      implicit loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = {
    inTransaction(_.createIDPConfig) { implicit connection =>
      withoutIDPConfig(identityProviderConfig.identityProviderId) {
        backend.createIdentityProviderConfig(identityProviderConfig)(connection)
        identityProviderConfig
      }
    }
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
    }
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
      id: IdentityProviderId.Id
  )(t: => T)(implicit connection: Connection): Result[T] = {
    backend.getIdentityProviderConfig(id = id)(connection) match {
      case Some(identityProviderConfig) =>
        Left(
          IdentityProviderConfigExists(identityProviderId =
            identityProviderConfig.identityProviderId
          )
        )
      case None => Right(t)
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
