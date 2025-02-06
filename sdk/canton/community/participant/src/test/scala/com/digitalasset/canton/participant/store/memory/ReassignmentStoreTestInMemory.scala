// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.ReassignmentStoreTest
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class ReassignmentStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ReassignmentStoreTest {

  private def mk(synchronizer: IndexedSynchronizer): InMemoryReassignmentStore =
    new InMemoryReassignmentStore(Target(synchronizer.synchronizerId), loggerFactory)

  "ReassignmentStoreTestInMemory" should {
    behave like reassignmentStore(mk)
  }
}
