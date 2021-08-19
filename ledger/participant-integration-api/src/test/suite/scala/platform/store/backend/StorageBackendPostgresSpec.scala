// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AsyncFlatSpec

final class StorageBackendPostgresSpec
    extends AsyncFlatSpec
    with StorageBackendProviderPostgres
    with StorageBackendSpec[StorageBackendProviderPostgres.DB_BATCH]
    with StorageBackendTestsInitialization[StorageBackendProviderPostgres.DB_BATCH]
    with StorageBackendTestsIngestion[StorageBackendProviderPostgres.DB_BATCH]
    with StorageBackendTestsReset[StorageBackendProviderPostgres.DB_BATCH]
    with StorageBackendTestsPruning[StorageBackendProviderPostgres.DB_BATCH]
