// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import java.sql.Connection

import com.daml.api.util.TimeProvider
import com.daml.lf.data.Ref
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.StorageBackendProvider
import com.daml.platform.store.backend.localstore.{
  PartyRecordStorageBackend,
  PartyRecordStorageBackendImpl,
  ResourceVersionOpsBackend,
}
import org.scalatest.freespec.AsyncFreeSpec

trait PersistentPartyRecordStoreTests
    extends PersistentStoreSpecBase
    with PartyRecordStoreTests
    with ConcurrentChangeControlTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

  override def newStore() = new PersistentPartyRecordStore(
    dbSupport = dbSupport,
    metrics = Metrics.ForTesting,
    timeProvider = TimeProvider.UTC,
    executionContext = executionContext,
  )

  override private[localstore] def testedResourceVersionBackend: ResourceVersionOpsBackend =
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
