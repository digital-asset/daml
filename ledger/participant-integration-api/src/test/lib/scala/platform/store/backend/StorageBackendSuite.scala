// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AsyncFlatSpec

trait StorageBackendSuite
    extends StorageBackendSpec
    with StorageBackendTestsInitialization
    with StorageBackendTestsInitializeIngestion
    with StorageBackendTestsIngestion
    with StorageBackendTestsCompletions
    with StorageBackendTestsReset
    with StorageBackendTestsPruning
    with StorageBackendTestsTimestamps {
  this: AsyncFlatSpec =>
}
