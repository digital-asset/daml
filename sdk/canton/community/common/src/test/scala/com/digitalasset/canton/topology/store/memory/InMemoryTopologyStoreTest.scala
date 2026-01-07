// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreTest}
import com.digitalasset.canton.version.HasTestCloseContext

class InMemoryTopologyStoreTest extends TopologyStoreTest with HasTestCloseContext {

  override def closeContext: CloseContext = testCloseContext

  "InMemoryTopologyStore" should {
    behave like topologyStore { case (synchronizerId, testName) =>
      new InMemoryTopologyStore(
        TopologyStoreId.SynchronizerStore(synchronizerId),
        testedProtocolVersion,
        loggerFactory.appendUnnamedKey("testName", testName),
        timeouts,
      )
    }
  }
}
