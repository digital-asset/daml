// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.partymanagement

import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryParticipantPartyRecordStore
import com.daml.ledger.participant.state.index.v2.ParticipantPartyRecordStore
import com.daml.platform.store.platform.partymanagement.ParticipantPartyStoreTests
import org.scalatest.Assertion
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

class InMemoryParticipantPartyRecordStoreSpec
    extends AsyncFreeSpec
    with ParticipantPartyStoreTests {

  override def testIt(f: ParticipantPartyRecordStore => Future[Assertion]): Future[Assertion] = {
    f(
      new InMemoryParticipantPartyRecordStore(
        executionContext = executionContext
      )
    )
  }

}
