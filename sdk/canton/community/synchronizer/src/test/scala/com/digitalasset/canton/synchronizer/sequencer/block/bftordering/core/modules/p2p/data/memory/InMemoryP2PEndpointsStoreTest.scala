// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.memory

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class InMemoryP2PEndpointsStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with P2PEndpointsStoreTest {

  "InMemoryP2pEndpointsStore" should {
    behave like p2pEndpointsStore(() => new InMemoryP2PEndpointsStore())
  }
}
