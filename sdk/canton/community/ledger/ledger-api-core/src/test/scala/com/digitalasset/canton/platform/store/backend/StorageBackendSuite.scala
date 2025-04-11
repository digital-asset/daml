// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import org.scalatest.flatspec.AnyFlatSpec

trait StorageBackendSuite
    extends StorageBackendTestsInitialization
    with StorageBackendTestsInitializeIngestion
    with StorageBackendTestsParties
    with StorageBackendTestsEvents
    with StorageBackendTestsTransactionStreamsEvents
    with StorageBackendTestsCompletions
    with StorageBackendTestsContracts
    with StorageBackendTestsReset
    with StorageBackendTestsPruning
    with StorageBackendTestsDBLockForSuite
    with StorageBackendTestsIntegrity
    with StorageBackendTestsTimestamps
    with StorageBackendTestsStringInterning
    with StorageBackendTestsUserManagement
    with StorageBackendTestsIDPConfig
    with StorageBackendTestsPartyRecord
    with StorageBackendTestsReassignmentEvents
    with StorageBackendTestsPartyToParticipant
    with StorageBackendTestsQueryValidRange
    with StorageBackendTestsParameters {
  this: AnyFlatSpec =>
}
