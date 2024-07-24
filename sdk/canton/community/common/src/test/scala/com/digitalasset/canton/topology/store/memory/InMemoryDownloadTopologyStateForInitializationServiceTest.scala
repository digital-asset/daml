// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  DownloadTopologyStateForInitializationServiceTest,
  TopologyStore,
  TopologyStoreId,
}

class InMemoryDownloadTopologyStateForInitializationServiceTest
    extends DownloadTopologyStateForInitializationServiceTest {
  override protected def createTopologyStore(
      domainId: DomainId
  ): TopologyStore[TopologyStoreId.DomainStore] = {
    val storeId = DomainStore(domainId, getClass.getSimpleName.take(40))
    new InMemoryTopologyStore[TopologyStoreId.DomainStore](storeId, loggerFactory, timeouts)
  }
}
