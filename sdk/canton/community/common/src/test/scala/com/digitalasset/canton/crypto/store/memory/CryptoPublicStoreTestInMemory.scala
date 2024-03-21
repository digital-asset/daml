// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import com.digitalasset.canton.crypto.store.CryptoPublicStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class CryptoPublicStoreTestInMemory extends AsyncWordSpec with CryptoPublicStoreTest {
  "InMemoryCryptoPublicStore" should {
    behave like cryptoPublicStore(new InMemoryCryptoPublicStore, backedByDatabase = false)
  }
}
