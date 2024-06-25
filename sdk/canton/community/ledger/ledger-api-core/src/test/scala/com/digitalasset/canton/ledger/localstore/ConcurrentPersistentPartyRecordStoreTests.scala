// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.platform.store.backend.StorageBackendProvider
import com.digitalasset.canton.platform.store.backend.localstore.{
  PartyRecordStorageBackend,
  PartyRecordStorageBackendImpl,
  ResourceVersionOps,
}
import org.scalatest.freespec.AsyncFreeSpec

import java.sql.Connection

trait ConcurrentPersistentPartyRecordStoreTests extends ConcurrentChangeControlTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

  override private[localstore] def testedResourceVersionBackend: ResourceVersionOps =
    PartyRecordStorageBackendImpl

  private[localstore] type ResourceId = Ref.Party
  private[localstore] type DbResource = PartyRecordStorageBackend.DbPartyRecord

  private[localstore] override def createAndGetNewResource(
      initialResourceVersion: Long
  )(connection: Connection): DbResource = {
    val id = Ref.Party.assertFromString("party1")
    PartyRecordStorageBackendImpl.createPartyRecord(
      PartyRecordStorageBackend.DbPartyRecordPayload(
        party = id,
        identityProviderId = None,
        resourceVersion = initialResourceVersion,
        createdAt = 0,
      )
    )(connection)
    PartyRecordStorageBackendImpl.getPartyRecord(id)(connection).value
  }

  private[localstore] override def fetchResourceVersion(
      id: ResourceId
  )(connection: Connection): Long = {
    PartyRecordStorageBackendImpl.getPartyRecord(id)(connection).value.payload.resourceVersion
  }

  private[localstore] override def getResourceVersion(resource: DbResource): Long = {
    resource.payload.resourceVersion
  }

  private[localstore] override def getId(resource: DbResource): ResourceId = resource.payload.party

  private[localstore] override def getDbInternalId(resource: DbResource): Int = resource.internalId

}
