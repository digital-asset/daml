// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.platform.store.backend.StorageBackendProviderH2
import org.scalatest.freespec.AsyncFreeSpec

class PersistentIdentityProviderConfigStoreH2Spec
    extends AsyncFreeSpec
    with PersistentIdentityProviderConfigStoreTests
    with StorageBackendProviderH2
