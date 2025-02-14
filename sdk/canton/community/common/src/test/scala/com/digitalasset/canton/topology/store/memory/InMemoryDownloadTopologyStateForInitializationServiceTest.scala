// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{
  DownloadTopologyStateForInitializationServiceTest,
  TopologyStore,
  TopologyStoreId,
}

class InMemoryDownloadTopologyStateForInitializationServiceTest
    extends DownloadTopologyStateForInitializationServiceTest {
  override protected def mkStore(
      synchronizerId: SynchronizerId
  ): TopologyStore[TopologyStoreId.SynchronizerStore] = {
    val storeId = SynchronizerStore(synchronizerId)
    new InMemoryTopologyStore[TopologyStoreId.SynchronizerStore](
      storeId,
      testedProtocolVersion,
      loggerFactory,
      timeouts,
    )
  }
}
