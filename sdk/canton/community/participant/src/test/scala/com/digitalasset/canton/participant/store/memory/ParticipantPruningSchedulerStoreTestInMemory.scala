// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory;

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.ParticipantPruningSchedulerStoreTest
import org.scalatest.wordspec.AsyncWordSpec;

class ParticipantPruningSchedulerStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with ParticipantPruningSchedulerStoreTest {

  "InMemoryParticipantPruningSchedulerStore" should {
    behave like participantPruningSchedulerStore(() =>
      new InMemoryParticipantPruningSchedulerStore(loggerFactory)
    )
  }
}
