// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.version.HasTestCloseContext
import org.scalatest.wordspec.AsyncWordSpec

class ActiveContractStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with ActiveContractStoreTest {

  override protected implicit lazy val closeContext: CloseContext =
    HasTestCloseContext.makeTestCloseContext(logger)

  /*
    We need 3 domains.
    In `DbActiveContractStoreTest`, we have an `acsDomainId` which takes index 1, so we start at 2.
   */
  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 2, maxIndex = 4)

  "InMemoryActiveContractStore" should {
    behave like activeContractStore(
      ec =>
        new InMemoryActiveContractStore(indexedStringStore, testedProtocolVersion, loggerFactory)(
          ec
        ),
      ec => new InMemoryContractStore(loggerFactory)(ec),
    )
  }

}
