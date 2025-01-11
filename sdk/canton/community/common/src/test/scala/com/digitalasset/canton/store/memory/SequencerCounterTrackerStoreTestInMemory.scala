// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.store.SequencerCounterTrackerStoreTest
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

class SequencerCounterTrackerStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SequencerCounterTrackerStoreTest
    with FlagCloseable
    with HasCloseContext
    with FailOnShutdown {

  "InMemorySequencerCounterTrackerStore" should {
    behave like sequencerCounterTrackerStore(() =>
      new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
    )
  }
}
