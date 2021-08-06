// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AsyncFlatSpec

final class StorageBackendH2Spec
    extends AsyncFlatSpec
    with StorageBackendProviderH2
    with StorageBackendSpec[StorageBackendProviderH2.DB_BATCH]
    with StorageBackendTestsInitialization[StorageBackendProviderH2.DB_BATCH]
    with StorageBackendTestsIngestion[StorageBackendProviderH2.DB_BATCH]
    with StorageBackendTestsReset[StorageBackendProviderH2.DB_BATCH]
    with StorageBackendTestsPruning[StorageBackendProviderH2.DB_BATCH]
