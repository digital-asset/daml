// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.platform.store.backend.StorageBackendProviderPostgres
import org.scalatest.freespec.AsyncFreeSpec

class PersistentUserStoreSpecPostgres
    extends AsyncFreeSpec
    with PersistentUserStoreTests
    with ConcurrentPersistentUserStoreTests
    with StorageBackendProviderPostgres
