// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.partymanagement

import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryPartyRecordStore
import com.daml.ledger.participant.state.index.v2.PartyRecordStore
import com.daml.platform.store.platform.partymanagement.PartyRecordStoreTests
import org.scalatest.freespec.AsyncFreeSpec

class InMemoryPartyRecordStoreSpec extends AsyncFreeSpec with PartyRecordStoreTests {

  override def newStore(): PartyRecordStore = new InMemoryPartyRecordStore(
    executionContext = executionContext
  )

}
