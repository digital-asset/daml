// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStoreTest
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class SynchronizerConnectionConfigStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SynchronizerConnectionConfigStoreTest
    with HasExecutionContext {
  "InMemorySynchronizerConnectionConfigStore" should {
    behave like synchronizerConnectionConfigStore(
      FutureUnlessShutdown.pure(
        new InMemorySynchronizerConnectionConfigStore(aliasManager, loggerFactory)
      )
    )
  }
}
