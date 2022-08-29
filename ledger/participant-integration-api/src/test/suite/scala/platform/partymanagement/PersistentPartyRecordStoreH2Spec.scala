// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.partymanagement

import com.daml.platform.store.backend.StorageBackendProviderH2
import com.daml.platform.store.platform.partymanagement.PersistentPartyRecordStoreTests
import org.scalatest.freespec.AsyncFreeSpec

class PersistentPartyRecordStoreH2Spec
    extends AsyncFreeSpec
    with PersistentPartyRecordStoreTests
    with StorageBackendProviderH2
