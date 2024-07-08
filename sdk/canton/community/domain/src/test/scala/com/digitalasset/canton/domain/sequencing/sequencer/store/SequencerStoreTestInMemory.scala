// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

class SequencerStoreTestInMemory extends AsyncWordSpec with BaseTest with SequencerStoreTest {
  "InMemorySequencerStore" should {
    behave like sequencerStore(() =>
      new InMemorySequencerStore(
        protocolVersion = testedProtocolVersion,
        sequencerMember = sequencerMember,
        unifiedSequencer = testedUseUnifiedSequencer,
        loggerFactory = loggerFactory,
      )
    )
  }
}
