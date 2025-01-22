// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStoreTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.InMemoryEpochStore
import org.scalatest.wordspec.AsyncWordSpec

class InMemoryEpochStoreTest extends AsyncWordSpec with BftSequencerBaseTest with EpochStoreTest {

  "InMemoryEpochStore" should {
    behave like epochStore(() => new InMemoryEpochStore())
  }
}
