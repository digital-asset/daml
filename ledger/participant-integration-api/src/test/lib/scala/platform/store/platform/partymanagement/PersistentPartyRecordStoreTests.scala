// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.platform.partymanagement

import com.codahale.metrics.MetricRegistry
import com.daml.api.util.TimeProvider
import com.daml.metrics.Metrics
import com.daml.platform.partymanagement.PersistentPartyRecordStore
import com.daml.platform.store.PersistentStoreSpecBase
import com.daml.platform.store.backend.StorageBackendProvider
import org.scalatest.freespec.AsyncFreeSpec

trait PersistentPartyRecordStoreTests extends PersistentStoreSpecBase with PartyRecordStoreTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

  override def newStore() = new PersistentPartyRecordStore(
    dbSupport = dbSupport,
    metrics = new Metrics(new MetricRegistry()),
    timeProvider = TimeProvider.UTC,
    executionContext = executionContext,
  )

}
