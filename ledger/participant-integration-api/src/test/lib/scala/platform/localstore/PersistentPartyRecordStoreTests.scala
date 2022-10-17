// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.api.util.TimeProvider
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.StorageBackendProvider
import org.scalatest.freespec.AsyncFreeSpec

trait PersistentPartyRecordStoreTests extends PersistentStoreSpecBase with PartyRecordStoreTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

  override def newStore() = new PersistentPartyRecordStore(
    dbSupport = dbSupport,
    metrics = Metrics.ForTesting,
    timeProvider = TimeProvider.UTC,
    executionContext = executionContext,
  )

}
