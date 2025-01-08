// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.network.data.memory

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.network.data.P2pEndpointsStoreTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.networking.data.memory.InMemoryP2pEndpointsStore
import org.scalatest.wordspec.AsyncWordSpec

class InMemoryP2pEndpointsStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with P2pEndpointsStoreTest {

  "InMemoryP2pEndpointsStore" should {
    behave like p2pEndpointsStore(() => new InMemoryP2pEndpointsStore())
  }
}
