// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreTest}

class InMemoryTopologyStoreTest extends TopologyStoreTest {

  "InMemoryPartyMetadataStore" should {
    behave like partyMetadataStore(() => new InMemoryPartyMetadataStore)
  }

  "InMemoryTopologyStore" should {
    behave like topologyStore(synchronizerId =>
      new InMemoryTopologyStore(
        TopologyStoreId.DomainStore(synchronizerId),
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      )
    )
  }
}
