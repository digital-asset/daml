// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AsyncFlatSpec

final class StorageBackendOracleSpec
    extends AsyncFlatSpec
    with StorageBackendProviderOracle
    with StorageBackendSpec[StorageBackendProviderOracle.DB_BATCH]
    with StorageBackendTestsInitialization[StorageBackendProviderOracle.DB_BATCH]
    with StorageBackendTestsIngestion[StorageBackendProviderOracle.DB_BATCH]
    with StorageBackendTestsReset[StorageBackendProviderOracle.DB_BATCH]
    with StorageBackendTestsPruning[StorageBackendProviderOracle.DB_BATCH]
