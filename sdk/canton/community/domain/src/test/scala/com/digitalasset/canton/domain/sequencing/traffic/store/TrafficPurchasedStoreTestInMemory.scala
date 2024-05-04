// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficPurchasedStore
import org.scalatest.wordspec.AsyncWordSpec

class TrafficPurchasedStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with TrafficPurchasedStoreTest {
  "InMemoryTrafficPurchasedStore" should {
    behave like trafficPurchasedStore(() => new InMemoryTrafficPurchasedStore(loggerFactory))
  }
}
