// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreXTest}

class InMemoryTopologyStoreXTest extends TopologyStoreXTest {

  "InMemoryTopologyStore" should {
    behave like topologyStore(() =>
      new InMemoryTopologyStoreX(
        TopologyStoreId.AuthorizedStore,
        loggerFactory,
        timeouts,
      )
    )
  }
}
