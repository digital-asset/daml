// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.data.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.synchronizer.block.data.memory.InMemorySequencerBlockStore
import com.digitalasset.canton.synchronizer.data.SequencerBlockStoreTest
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.topology.{Namespace, SequencerId}
import org.scalatest.wordspec.AsyncWordSpec

class SequencerBlockStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SequencerBlockStoreTest {
  "InMemorySequencerStore" should {
    val sequencerId =
      SequencerId.tryCreate("sequencer", Namespace(Fingerprint.tryFromString("default")))
    behave like sequencerBlockStore {
      val sequencerStore = new InMemorySequencerStore(
        testedProtocolVersion,
        sequencerId,
        blockSequencerMode = true,
        loggerFactory,
        sequencerMetrics = SequencerMetrics.noop("db-sequencer-block-store-test"),
      )

      val blockStore = new InMemorySequencerBlockStore(sequencerStore, loggerFactory)
      (sequencerStore, blockStore)
    }

  }

}
