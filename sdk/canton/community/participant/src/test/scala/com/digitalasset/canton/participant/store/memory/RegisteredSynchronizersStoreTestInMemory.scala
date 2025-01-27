// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.RegisteredSynchronizersStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class RegisteredSynchronizersStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with RegisteredSynchronizersStoreTest {
  "InMemoryRegisteredSynchronizersStore" should {
    behave like registeredSynchronizersStore(() =>
      new InMemoryRegisteredSynchronizersStore(loggerFactory)
    )
  }
}
