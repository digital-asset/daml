// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.store.InMemorySequencerStore
import org.scalatest.wordspec.AsyncWordSpec

class SequencerStoreTestInMemory extends AsyncWordSpec with BaseTest with SequencerStoreTest {

  "InMemorySequencerStore" should {
    behave like sequencerStore(() =>
      new InMemorySequencerStore(
        protocolVersion = testedProtocolVersion,
        sequencerMember = sequencerMember,
        blockSequencerMode = true,
        loggerFactory = loggerFactory,
      )
    )
  }
}
