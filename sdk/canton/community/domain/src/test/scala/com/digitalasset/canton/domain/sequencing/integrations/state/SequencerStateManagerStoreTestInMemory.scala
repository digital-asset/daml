// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

class SequencerStateManagerStoreTestInMemory extends SequencerStateManagerStoreTest {
  "InMemorySequencerStateManagerStore" should {
    behave like sequencerStateManagerStore(() =>
      new InMemorySequencerStateManagerStore(loggerFactory)
    )
  }
}
