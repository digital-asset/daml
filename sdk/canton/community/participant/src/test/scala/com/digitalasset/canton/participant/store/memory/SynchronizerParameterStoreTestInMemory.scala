// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.DomainParameterStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class SynchronizerParameterStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with DomainParameterStoreTest {

  "InMemoryDomainParameterStore" should {
    behave like domainParameterStore(_ => new InMemorySynchronizerParameterStore())
  }
}
