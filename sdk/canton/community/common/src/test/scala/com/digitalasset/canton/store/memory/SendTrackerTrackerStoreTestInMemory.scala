// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.store.SendTrackerStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class SendTrackerTrackerStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SendTrackerStoreTest {
  "InMemoryPendingSendStore" should {
    behave like sendTrackerStore(() => new InMemorySendTrackerStore)
  }
}
