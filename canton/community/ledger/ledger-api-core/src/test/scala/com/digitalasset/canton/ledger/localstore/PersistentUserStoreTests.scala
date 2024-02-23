// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain.IdentityProviderConfig
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.ledger.localstore.{
  PersistentIdentityProviderConfigStore,
  PersistentUserManagementStore,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.backend.StorageBackendProvider
import com.digitalasset.canton.platform.store.backend.localstore.UserManagementStorageBackend.DbUserPayload
import com.digitalasset.canton.platform.store.backend.localstore.{
  ResourceVersionOps,
  UserManagementStorageBackend,
  UserManagementStorageBackendImpl,
}
import org.scalatest.freespec.AsyncFreeSpec

import java.sql.Connection
import scala.concurrent.Future

trait PersistentUserStoreTests
    extends PersistentStoreSpecBase
    with UserStoreTests
    with ConcurrentChangeControlTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

  override def newStore(): UserManagementStore =
    new PersistentUserManagementStore(
      dbSupport = dbSupport,
      metrics = Metrics.ForTesting,
      timeProvider = TimeProvider.UTC,
      maxRightsPerUser = 100,
      loggerFactory = loggerFactory,
    )

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig): Future[Unit] = {
    new PersistentIdentityProviderConfigStore(dbSupport, Metrics.ForTesting, 10, loggerFactory)
      .createIdentityProviderConfig(identityProviderConfig)(loggingContext)
      .flatMap {
        case Left(error) => Future.failed(new Exception(error.toString))
        case Right(_) => Future.unit
      }
  }

  override private[localstore] def testedResourceVersionBackend: ResourceVersionOps =
    UserManagementStorageBackendImpl

  private[localstore] type ResourceId = Ref.UserId
  private[localstore] type DbResource = UserManagementStorageBackend.DbUserWithId

  private[localstore] override def createAndGetNewResource(
      initialResourceVersion: Long
  )(connection: Connection): DbResource = {
    val id = Ref.UserId.assertFromString("user1")
    UserManagementStorageBackendImpl.createUser(
      DbUserPayload(
        id = id,
        primaryPartyO = None,
        identityProviderId = None,
        isDeactivated = false,
        resourceVersion = initialResourceVersion,
        createdAt = 0,
      )
    )(connection)
    UserManagementStorageBackendImpl.getUser(id)(connection).value
  }

  private[localstore] override def fetchResourceVersion(
      id: ResourceId
  )(connection: Connection): Long = {
    UserManagementStorageBackendImpl.getUser(id)(connection).value.payload.resourceVersion
  }

  private[localstore] override def getResourceVersion(resource: DbResource): Long = {
    resource.payload.resourceVersion
  }

  private[localstore] override def getId(resource: DbResource): ResourceId = resource.payload.id

  private[localstore] override def getDbInternalId(resource: DbResource): Int = resource.internalId

}
