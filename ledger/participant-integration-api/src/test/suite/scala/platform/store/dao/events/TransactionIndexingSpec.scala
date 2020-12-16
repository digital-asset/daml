// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import com.daml.ledger
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.test.TransactionBuilder
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TransactionIndexingSpec extends AnyWordSpec with Matchers {
  "TransactionIndexing" should {
    "apply blindingInfo divulgence upon construction" in {
      val aContractId = ContractId.assertFromString("#c0")
      val aDivulgedParty = Party.assertFromString("aParty")
      val aDivulgence = Map(aContractId -> Set(aDivulgedParty))
      val anOffset = Offset.fromByteArray(Array.emptyByteArray)
      val aTransactionId = ledger.TransactionId.assertFromString("0")

      val anInstant = Instant.EPOCH
      val aCommittedTransaction = TransactionBuilder().buildCommitted()

      TransactionIndexing
        .from(
          blindingInfo = BlindingInfo(Map.empty, aDivulgence),
          submitterInfo = None,
          workflowId = None,
          aTransactionId,
          anInstant,
          anOffset,
          aCommittedTransaction,
          Iterable.empty,
        )
        .contractWitnesses should be(
        TransactionIndexing
          .ContractWitnessesInfo(Set.empty, aDivulgence)
      )
    }
  }
}
