// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.participant.store.RequestJournalStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class RequestJournalStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with RequestJournalStoreTest
    with FlagCloseable
    with HasCloseContext {
  "InMemoryRequestJournalStore" should {
    behave like requestJournalStore(() => new InMemoryRequestJournalStore(loggerFactory))
  }
}
