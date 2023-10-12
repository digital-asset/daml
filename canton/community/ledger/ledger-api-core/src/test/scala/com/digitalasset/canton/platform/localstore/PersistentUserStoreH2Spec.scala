// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore

import com.digitalasset.canton.platform.store.backend.StorageBackendProviderH2
import org.scalatest.freespec.AsyncFreeSpec

class PersistentUserStoreH2Spec
    extends AsyncFreeSpec
    with PersistentUserStoreTests
    with StorageBackendProviderH2
