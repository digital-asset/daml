// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.platform.store.backend.StorageBackendProviderH2
import org.scalatest.freespec.AsyncFreeSpec

class PersistentUserStoreSpecH2
    extends AsyncFreeSpec
    with PersistentUserStoreTests
    with StorageBackendProviderH2
