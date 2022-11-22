// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.platform.localstore.PersistentStoreSpecBase
import com.daml.platform.store.backend.StorageBackendProviderPostgres
import org.scalatest.freespec.AsyncFreeSpec

class DbDispatcherPostgresSpec
    extends AsyncFreeSpec
    with PersistentStoreSpecBase
    with DbDispatcherTests
    with StorageBackendProviderPostgres {}
