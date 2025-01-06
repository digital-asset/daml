// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.memory.InMemoryTrafficConsumedStore
import com.digitalasset.canton.topology.Member
import org.scalatest.wordspec.AsyncWordSpec

class TrafficConsumedStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with TrafficConsumedStoreTest {
  override def registerMemberInSequencerStore(member: Member): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit

  "InMemoryTrafficPurchasedStore" should {
    behave like trafficConsumedStore(() => new InMemoryTrafficConsumedStore(loggerFactory))
  }
}
