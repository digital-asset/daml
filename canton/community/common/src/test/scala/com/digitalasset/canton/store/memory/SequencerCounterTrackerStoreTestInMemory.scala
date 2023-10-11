// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.store.SequencerCounterTrackerStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class SequencerCounterTrackerStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SequencerCounterTrackerStoreTest
    with FlagCloseable
    with HasCloseContext {

  "InMemorySequencerCounterTrackerStore" should {
    behave like sequencerCounterTrackerStore(() =>
      new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
    )
  }
}
