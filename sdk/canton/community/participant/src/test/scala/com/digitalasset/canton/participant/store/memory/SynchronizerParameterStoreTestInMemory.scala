// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.SynchronizerParameterStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class SynchronizerParameterStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SynchronizerParameterStoreTest {

  "InMemorySynchronizerParameterStore" should {
    behave like synchronizerParameterStore(_ => new InMemorySynchronizerParameterStore())
  }
}
