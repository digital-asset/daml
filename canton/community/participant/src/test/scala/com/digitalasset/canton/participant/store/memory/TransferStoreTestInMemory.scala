// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.TransferStoreTest
import com.digitalasset.canton.protocol.TargetDomainId
import org.scalatest.wordspec.AsyncWordSpec

class TransferStoreTestInMemory extends AsyncWordSpec with BaseTest with TransferStoreTest {

  private def mk(domain: TargetDomainId): InMemoryTransferStore =
    new InMemoryTransferStore(domain, loggerFactory)

  "TransferStoreTestInMemory" should {
    behave like transferStore(mk)
  }
}
