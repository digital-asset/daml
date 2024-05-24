// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.TransferStoreTest
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class TransferStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with TransferStoreTest {

  private def mk(domain: TargetDomainId): InMemoryTransferStore =
    new InMemoryTransferStore(domain, loggerFactory)

  "TransferStoreTestInMemory" should {
    behave like transferStore(mk)
  }
}
