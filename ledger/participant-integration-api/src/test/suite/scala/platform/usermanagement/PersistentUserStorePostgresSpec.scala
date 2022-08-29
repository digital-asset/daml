// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import com.daml.platform.store.backend.StorageBackendProviderPostgres
import com.daml.platform.store.platform.usermanagement.PersistentUserStoreTests
import org.scalatest.freespec.AsyncFreeSpec

class PersistentUserStorePostgresSpec
    extends AsyncFreeSpec
    with PersistentUserStoreTests
    with StorageBackendProviderPostgres
