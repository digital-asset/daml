// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreTest}

class InMemoryTopologyStoreTest extends TopologyStoreTest {

  "InMemoryTopologyStore" should {
    behave like topologyStore(() =>
      new InMemoryTopologyStore(
        TopologyStoreId.AuthorizedStore,
        loggerFactory,
        timeouts,
      )
    )
  }
}
