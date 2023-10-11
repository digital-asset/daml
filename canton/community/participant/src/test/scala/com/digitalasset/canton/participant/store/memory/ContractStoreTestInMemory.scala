// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.*
import org.scalatest.wordspec.AsyncWordSpec

class ContractStoreTestInMemory extends AsyncWordSpec with BaseTest with ContractStoreTest {

  "InMemoryContractStore" should {
    behave like contractStore(() => new InMemoryContractStore(loggerFactory))
  }

}
