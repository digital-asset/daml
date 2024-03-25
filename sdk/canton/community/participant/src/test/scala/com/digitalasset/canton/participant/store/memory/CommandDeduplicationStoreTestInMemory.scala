// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.CommandDeduplicationStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class CommandDeduplicationStoreTestInMemory
    extends AsyncWordSpec
    with CommandDeduplicationStoreTest {

  "InMemoryCommandDeduplicationStore" should {
    behave like (commandDeduplicationStore(() =>
      new InMemoryCommandDeduplicationStore(loggerFactory)
    ))
  }
}
