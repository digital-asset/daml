// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory;

import com.digitalasset.canton.store.PruningSchedulerStoreTest
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec;

class PruningSchedulerStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with PruningSchedulerStoreTest
    with FailOnShutdown {

  "InMemoryPruningSchedulerStore" should {
    behave like pruningSchedulerStore(() => new InMemoryPruningSchedulerStore(loggerFactory))
  }
}
