// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.ledger.localstore.PersistentIdentityProviderConfigStore
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.backend.StorageBackendProvider
import org.scalatest.freespec.AsyncFreeSpec

trait PersistentIdentityProviderConfigStoreTests
    extends PersistentStoreSpecBase
    with IdentityProviderConfigStoreTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

  override def newStore() = new PersistentIdentityProviderConfigStore(
    dbSupport = dbSupport,
    metrics = Metrics.ForTesting,
    maxIdentityProviders = MaxIdentityProviderConfigs,
    loggerFactory = loggerFactory,
  )

}
