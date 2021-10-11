// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import com.daml.ledger.offset.Offset
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.ledger.BlindingTransaction
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.{Value => V}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TransactionIndexingSpec extends AnyWordSpec with Matchers {

  import TransactionBuilder.Implicits._

  private val anOffset = Offset.fromByteArray(Array.emptyByteArray)
  private val aTransactionId = Ref.TransactionId.assertFromString("0")
  private val anInstant = Instant.EPOCH

  "TransactionIndexing" should {
    "apply blindingInfo divulgence upon construction" in {
      val aContractId = ContractId.assertFromString("#c0")
      val aDivulgedParty = Party.assertFromString("aParty")
      val aDivulgence = Map(aContractId -> Set(aDivulgedParty))

      val aCommittedTransaction = TransactionBuilder().buildCommitted()

      TransactionIndexing
        .from(
          blindingInfo = BlindingInfo(Map.empty, aDivulgence),
          completionInfo = None,
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

    "filter out rollback nodes" in {
      val partyStr = "Alice"
      val party = Party.assertFromString(partyStr)
      val bobStr = "Bob"
      val bob = Party.assertFromString(bobStr)

      def create(builder: TransactionBuilder, id: String) =
        builder.create(
          id = id,
          templateId = "M:T",
          argument = V.ValueRecord(None, ImmArray.Empty),
          signatories = Seq(partyStr),
          observers = Seq(),
          key = None,
        )

      def exercise(
          builder: TransactionBuilder,
          id: String,
          parties: Set[String] = Set(partyStr),
      ) = {
        builder.exercise(
          contract = create(builder, id),
          choice = "C",
          consuming = true,
          actingParties = parties,
          argument = V.ValueRecord(None, ImmArray.Empty),
        )
      }

      def fetch(builder: TransactionBuilder, id: String) =
        builder.fetch(create(builder, id))

      val builder = TransactionBuilder()

      val rootExe = exercise(builder, "#0", Set(bobStr))
      val rootExeId = builder.add(rootExe)
      val c1 = create(builder, "#1")
      val c1Id = builder.add(c1, rootExeId)
      // Divulgence of archived local contract.
      val e1 = exercise(builder, "#1")
      val e1Id = builder.add(e1, rootExeId)
      val c2 = create(builder, "#2")
      val c2Id = builder.add(c2, rootExeId)
      val rollback = builder.add(builder.rollback(), rootExeId)
      builder.add(create(builder, "#3"), rollback)
      // Divulgence of global contract via a rolled-back archive
      builder.add(exercise(builder, "#4"), rollback)
      builder.add(create(builder, "#5"), rollback)
      // Divulgence of contract created in rollback should not be included.
      builder.add(exercise(builder, "#5"), rollback)
      // Divulgence of global contract in rollback should be included.
      builder.add(fetch(builder, "#6"), rollback)
      val fId = builder.add(fetch(builder, "#7"), rootExeId)

      val blindingInfo = BlindingTransaction.calculateBlindingInfo(builder.build())

      val result = TransactionIndexing
        .from(
          blindingInfo = blindingInfo,
          completionInfo = None,
          workflowId = None,
          aTransactionId,
          anInstant,
          anOffset,
          builder.buildCommitted(),
          Iterable.empty,
        )

      val expectedArchives = Set(rootExe.targetCoid)

      result.contractWitnesses shouldBe TransactionIndexing.ContractWitnessesInfo(
        netArchives = expectedArchives,
        netVisibility = Seq[(String, Set[Party])](
          // Create disclosed to both.
          "#2" -> Set(party, bob),
          // Divulgene in rollback
          "#4" -> Set(bob),
          // Divulgence in rollback
          "#6" -> Set(bob),
          // Divulgence via fetch
          "#7" -> Set(bob),
        ).map { case (k, v) => (ContractId.assertFromString(k), v) }.toMap,
      )

      result.contracts shouldBe TransactionIndexing.ContractsInfo(
        netArchives = expectedArchives,
        netCreates = Set(c2),
        divulgedContracts = Iterable.empty,
      )

      result.events shouldBe TransactionIndexing
        .EventsInfo(
          // Everything under the rollback node is filtered out.
          events = Vector(
            (rootExeId, rootExe.copy(children = ImmArray(c1Id, e1Id, c2Id, rollback, fId))),
            (c1Id, c1),
            (e1Id, e1),
            (c2Id, c2),
          ),
          // Includes all archives outside of rollback nodes.
          archives = Set(rootExe.targetCoid, c1.coid),
          // Stakeholders of everything outside of rollback nodes.
          stakeholders = Map(
            rootExeId -> Set(party),
            c1Id -> Set(party),
            e1Id -> Set(party),
            c2Id -> Set(party),
          ),
          // Disclosure for non-rollback and non-fetch/lookup nodes.
          disclosure = Map(
            rootExeId -> Set(party, bob),
            c1Id -> Set(party, bob),
            e1Id -> Set(party, bob),
            c2Id -> Set(party, bob),
          ),
        )
    }
  }
}
