// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.platform.store.backend.StorageBackendProvider
import com.digitalasset.canton.platform.store.backend.localstore.UserManagementStorageBackend.DbUserPayload
import com.digitalasset.canton.platform.store.backend.localstore.{
  ResourceVersionOps,
  UserManagementStorageBackend,
  UserManagementStorageBackendImpl,
}
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.freespec.AsyncFreeSpec

import java.sql.Connection

trait ConcurrentPersistentUserStoreTests extends ConcurrentChangeControlTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

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
  )(connection: Connection): Long =
    UserManagementStorageBackendImpl.getUser(id)(connection).value.payload.resourceVersion

  private[localstore] override def getResourceVersion(resource: DbResource): Long =
    resource.payload.resourceVersion

  private[localstore] override def getId(resource: DbResource): ResourceId = resource.payload.id

  private[localstore] override def getDbInternalId(resource: DbResource): Int = resource.internalId

}
