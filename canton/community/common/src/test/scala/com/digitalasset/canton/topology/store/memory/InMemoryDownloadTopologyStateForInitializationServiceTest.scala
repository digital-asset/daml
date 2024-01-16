// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  DownloadTopologyStateForInitializationServiceTest,
  TopologyStoreId,
  TopologyStoreX,
}

class InMemoryDownloadTopologyStateForInitializationServiceTest
    extends DownloadTopologyStateForInitializationServiceTest {
  override protected def createTopologyStore(): TopologyStoreX[TopologyStoreId.DomainStore] = {
    val storeId = DomainStore(DefaultTestIdentities.domainId, getClass.getSimpleName.take(40))
    new InMemoryTopologyStoreX[TopologyStoreId.DomainStore](storeId, loggerFactory, timeouts)
  }
}
