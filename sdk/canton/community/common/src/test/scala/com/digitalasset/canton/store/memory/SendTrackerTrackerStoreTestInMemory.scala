// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.store.SendTrackerStoreTest
import org.scalatest.wordspec.AnyWordSpec

class SendTrackerTrackerStoreTestInMemory
    extends AnyWordSpec
    with BaseTest
    with SendTrackerStoreTest {
  "InMemoryPendingSendStore" should {
    behave like sendTrackerStore(() => new InMemorySendTrackerStore)
  }
}
