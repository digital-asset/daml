// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.participant.store.RequestJournalStoreTest
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

class RequestJournalStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with RequestJournalStoreTest
    with FlagCloseable
    with HasCloseContext
    with FailOnShutdown {
  "InMemoryRequestJournalStore" should {
    behave like requestJournalStore(() => new InMemoryRequestJournalStore(loggerFactory))
  }
}
