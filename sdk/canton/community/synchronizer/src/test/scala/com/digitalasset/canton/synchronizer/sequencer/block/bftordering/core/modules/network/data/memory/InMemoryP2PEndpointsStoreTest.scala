// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.network.data.memory

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.network.data.P2PEndpointsStoreTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.memory.InMemoryP2PEndpointsStore
import org.scalatest.wordspec.AsyncWordSpec

class InMemoryP2PEndpointsStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with P2PEndpointsStoreTest {

  "InMemoryP2pEndpointsStore" should {
    behave like p2pEndpointsStore(() => new InMemoryP2PEndpointsStore())
  }
}
