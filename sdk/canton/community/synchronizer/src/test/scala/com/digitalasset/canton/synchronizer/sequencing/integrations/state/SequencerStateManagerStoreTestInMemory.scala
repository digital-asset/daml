// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.integrations.state

class SequencerStateManagerStoreTestInMemory extends SequencerStateManagerStoreTest {
  "InMemorySequencerStateManagerStore" should {
    behave like sequencerStateManagerStore(() =>
      new InMemorySequencerStateManagerStore(loggerFactory)
    )
  }
}
