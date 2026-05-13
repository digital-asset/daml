// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.memory

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.BftOrdererPruningSchedulerStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class InMemoryBftOrdererPruningSchedulerStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with BftOrdererPruningSchedulerStoreTest {

  "InMemoryBftOrdererPruningSchedulerStore" should {
    behave like pruningSchedulerStore(() =>
      new InMemoryBftOrdererPruningSchedulerStore(loggerFactory)
    )
    behave like bftOrdererPruningSchedulerStore(() =>
      new InMemoryBftOrdererPruningSchedulerStore(loggerFactory)
    )
  }

}
