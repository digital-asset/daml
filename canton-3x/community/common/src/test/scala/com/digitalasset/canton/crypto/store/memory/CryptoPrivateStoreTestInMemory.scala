// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtendedTest
import org.scalatest.wordspec.AsyncWordSpec

class CryptoPrivateStoreTestInMemory extends AsyncWordSpec with CryptoPrivateStoreExtendedTest {
  "InMemoryCryptoPrivateStore" should {
    behave like cryptoPrivateStoreExtended(
      new InMemoryCryptoPrivateStore(testedReleaseProtocolVersion, loggerFactory),
      encrypted = false,
    )
  }
}
