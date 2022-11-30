// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.localstore.api.UserManagementStore
import com.daml.platform.store.backend.StorageBackendProvider
import com.daml.platform.store.backend.localstore.UserManagementStorageBackend.DbUserPayload
import com.daml.platform.store.backend.localstore.{
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
    )

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig): Future[Unit] = {
    new PersistentIdentityProviderConfigStore(dbSupport, Metrics.ForTesting, 10)
      .createIdentityProviderConfig(identityProviderConfig)(LoggingContext.ForTesting)
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
