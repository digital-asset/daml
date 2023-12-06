// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.{BaseTest, TestMetrics}
import org.scalatest.wordspec.AsyncWordSpec

class ActiveContractStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with ActiveContractStoreTest
    with TestMetrics {

  "InMemoryActiveContractStore" should {
    behave like activeContractStore(
      ec => new InMemoryActiveContractStore(testedProtocolVersion, loggerFactory)(ec),
      ec => new InMemoryContractStore(loggerFactory)(ec),
    )
  }

}
