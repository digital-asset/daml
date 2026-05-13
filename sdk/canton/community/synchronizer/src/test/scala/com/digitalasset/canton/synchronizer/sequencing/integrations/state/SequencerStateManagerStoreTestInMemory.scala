// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.integrations.state

import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.topology.{DefaultTestIdentities, SequencerId}

class SequencerStateManagerStoreTestInMemory extends SequencerStateManagerStoreTest {
  "InMemorySequencerStateManagerStore" should {
    behave like sequencerStateManagerStore(() =>
      (
        new InMemorySequencerStateManagerStore(loggerFactory),
        new InMemorySequencerStore(
          testedProtocolVersion,
          SequencerId(DefaultTestIdentities.physicalSynchronizerId.uid),
          true,
          loggerFactory,
          SequencerMetrics.noop(getClass.getName),
        ),
      )
    )
  }
}
