// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import com.daml.ledger
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.transaction.{BlindingInfo, TransactionVersion}
import com.daml.lf.transaction.Node.NodeCreate
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.{Value => LValue}
import com.daml.platform.indexer.CurrentOffset
import com.daml.platform.store.dao.events.TransactionIndexing.{
  ContractWitnessesInfo,
  ContractsInfo,
  EventsInfo,
  EventsInfoBatch,
}
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TransactionIndexingSpec extends AnyWordSpec with Matchers with MockitoSugar {
  "TransactionIndexing" should {
    "apply blindingInfo divulgence upon construction" in {
      val aContractId = ContractId.assertFromString("#c0")
      val aDivulgedParty = Party.assertFromString("aParty")
      val aDivulgence = Map(aContractId -> Set(aDivulgedParty))
      val anOffset = CurrentOffset(Offset.fromByteArray((1 to 5).map(_.toByte).toArray))
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

  classOf[TransactionIndexing].getSimpleName should {
    "combine" in {
      val leftEvents =
        (1 to 3).map(id => LedgerString.assertFromString(s"tId$id") -> mock[EventsInfo])
      val rightEvents =
        (4 to 6).map(id => LedgerString.assertFromString(s"tId$id") -> mock[EventsInfo])
      val Seq((c1, t1), (c2, t2), (c3, _)) = (1 to 3).map(_ => mock[ContractId] -> Instant.now())
      val archivedContracts = Set(c1, c3)
      val Seq(party1, party2, party3, party4, party5, party6) =
        (1 to 6).map(id => s"party$id").map(Party.assertFromString)
      val leftVisibility: WitnessRelation[ContractId] = Map(
        c1 -> Set(party1),
        c2 -> Set(party2, party3),
        c3 -> Set(party5, party6),
      )
      val rightVisibility: WitnessRelation[ContractId] = Map(
        c1 -> Set(party4),
        c2 -> Set(party4),
      )

      val leftContracts = ContractsInfo(
        netCreates = Map(nodeCreate(c1) -> t1),
        archives = Set.empty,
        divulgedContracts = Iterable.empty,
      )

      val rightContracts = ContractsInfo(
        netCreates = Map(nodeCreate(c2) -> t2),
        archives = Set(c1),
        divulgedContracts = Iterable.empty,
      )

      val leftWitnesses = ContractWitnessesInfo(
        archives = Set(c3),
        netVisibility = leftVisibility,
      )

      val rightWitnesses = ContractWitnessesInfo(
        archives = Set(c1),
        netVisibility = rightVisibility,
      )

      val left = TransactionIndexing(
        events = EventsInfoBatch(leftEvents.toList),
        contracts = leftContracts,
        contractWitnesses = leftWitnesses,
      )

      val right = TransactionIndexing(
        events = EventsInfoBatch(rightEvents.toList),
        contracts = rightContracts,
        contractWitnesses = rightWitnesses,
      )

      val combined = TransactionIndexing.combine(left, right)
      combined shouldBe TransactionIndexing(
        events = EventsInfoBatch((leftEvents ++ rightEvents).toList),
        contracts = ContractsInfo(
          archives = Set(c1),
          netCreates = Map(nodeCreate(c2) -> t2),
          divulgedContracts = Iterable.empty,
        ),
        contractWitnesses = ContractWitnessesInfo(
          archives = archivedContracts,
          netVisibility = Map(
            c2 -> Set(party2, party3, party4)
          ),
        ),
      )
    }
  }

  private def nodeCreate(contractId: ContractId): Create =
    NodeCreate[ContractId](
      coid = contractId,
      templateId = Identifier.assertFromString("template:some:test"),
      arg = LValue.ValueNone,
      agreementText = "",
      optLocation = None,
      signatories = Set.empty,
      stakeholders = Set.empty,
      key = None,
      version = TransactionVersion.VDev,
    )
}
