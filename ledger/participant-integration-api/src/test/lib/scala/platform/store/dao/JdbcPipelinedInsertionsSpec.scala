// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.UUID

import com.daml.ledger.EventId
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.Node.NodeCreate
import com.daml.lf.value.Value.ContractId
import com.daml.platform.ApiOffset
import com.daml.platform.indexer.{CurrentOffset, OffsetStep}
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside, LoneElement, OptionValues}

import scala.concurrent.Future

trait JdbcPipelinedInsertionsSpec extends Inside with OptionValues with Matchers with LoneElement {
  self: AsyncFlatSpec with JdbcLedgerDaoSuite with JdbcPipelinedTransactionInsertion =>
  private val ok = io.grpc.Status.Code.OK.value()
  behavior of "JdbcLedgerDao (on PostgreSQL)"

  it should "serialize a batch of transactions with contracts that are archived within the same batch" in {
    val divulgedContractId = ContractId.assertFromString(s"#divulged-${UUID.randomUUID}")

    // This test writes a batch of transactions that create one regular and one divulged
    // contract that are archived within the same batch.
    for {
      from <- ledgerDao.lookupLedgerEnd()
      // TX1: create a single contract that will stay active
      templateId = testIdentifier("NonTransientContract")
      (offset1, tx1) = singleCreate(create(_, Set(alice), templateId), List(alice))
      contractId = nonTransient(tx1).loneElement
      // TX2: create a single contract that will be archived within the same batch
      (offset2, tx2) = singleCreate
      transientContractId = nonTransient(tx2).loneElement
      // TX3: archive contract created in TX2
      (offset3, tx3) = singleExercise(transientContractId)
      // TX4: divulge a contract
      (offset4, tx4) = emptyTransaction(alice)
      // TX5: archive previously divulged contract
      (offset5, tx5) = singleExercise(divulgedContractId)
      _ <- storeBatch(
        List(
          //
          txEntry(CurrentOffset(offset1), tx1, Map.empty),
          txEntry(CurrentOffset(offset2), tx2, Map.empty, Option.empty[BlindingInfo]),
          txEntry(CurrentOffset(offset3), tx3, Map.empty, Option.empty[BlindingInfo]),
          txEntry(
            CurrentOffset(offset4),
            tx4,
            Map((divulgedContractId, someVersionedContractInstance) -> Set(charlie)),
          ),
          txEntry(CurrentOffset(offset5), tx5, Map.empty, Option.empty[BlindingInfo]),
        )
      )
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
      contractLookup <- ledgerDao.lookupActiveOrDivulgedContract(contractId, Set(alice))
      transientLookup <- ledgerDao.lookupActiveOrDivulgedContract(transientContractId, Set(alice))
      divulgedLookup <- ledgerDao.lookupActiveOrDivulgedContract(divulgedContractId, Set(alice))
    } yield {
      completions should contain allOf (
        tx1.commandId.get -> ok,
        tx2.commandId.get -> ok,
        tx3.commandId.get -> ok,
        tx4.commandId.get -> ok,
        tx5.commandId.get -> ok,
      )
      contractLookup.value.template shouldBe templateId

      transientLookup shouldBe None
      divulgedLookup shouldBe None
    }
  }

  private def txEntry(
      offsetStep: OffsetStep,
      tx: LedgerEntry.Transaction,
      divulgedContracts: Map[(ContractId, v1.ContractInst), Set[Party]],
      blindingInfo: Option[BlindingInfo] = None,
  ) =
    (
      (offsetStep, tx),
      divulgedContracts.keysIterator.map(c => v1.DivulgedContract(c._1, c._2)).toList,
      blindingInfo,
    )

  it should "allow idempotent transaction insertions" in {
    val key = "some-key"
    val create @ (offset, tx) = txCreateContractWithKey(alice, key, Some("1337"))
    val maybeSubmitterInfo = submitterInfo(tx)
    val preparedInsert = prepareInsert(maybeSubmitterInfo, tx, CurrentOffset(offset))
    for {
      _ <- ledgerDao.storeTransactionEvents(preparedInsert)
      // Assume the indexer restarts after events insertion
      _ <- ledgerDao.storeTransactionEvents(preparedInsert)
      _ <- ledgerDao.storeTransactionState(preparedInsert)
      // Assume the indexer restarts after state insertion
      _ <- store(create) // The whole transaction insertion succeeds
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, tx.actAs.toSet)
    } yield {
      inside(result.value.transaction) { case Some(transaction) =>
        transaction.commandId shouldBe tx.commandId.get
        transaction.offset shouldBe ApiOffset.toApiString(offset)
        transaction.effectiveAt.value.seconds shouldBe tx.ledgerEffectiveTime.getEpochSecond
        transaction.effectiveAt.value.nanos shouldBe tx.ledgerEffectiveTime.getNano
        transaction.transactionId shouldBe tx.transactionId
        transaction.workflowId shouldBe tx.workflowId.getOrElse("")
        inside(transaction.events.loneElement.event.created) { case Some(created) =>
          val (nodeId, createNode: NodeCreate[ContractId]) =
            tx.transaction.nodes.head
          created.eventId shouldBe EventId(tx.transactionId, nodeId).toLedgerString
          created.witnessParties should contain only (tx.actAs: _*)
          created.agreementText.getOrElse("") shouldBe createNode.coinst.agreementText
          created.contractKey shouldNot be(None)
          created.createArguments shouldNot be(None)
          created.signatories should contain theSameElementsAs createNode.signatories
          created.observers should contain theSameElementsAs createNode.stakeholders.diff(
            createNode.signatories
          )
          created.templateId shouldNot be(None)
        }
      }
    }
  }

  it should "not retrieve a transaction with an offset past the ledger end (when requested by single party)" in {
    assertNoResponseFor(singleCreate)
  }

  it should "not retrieve a transaction with an offset past the ledger end (when requested by multiple parties)" in {
    assertNoResponseFor(multiPartySingleCreate)
  }

  private def assertNoResponseFor(
      offsetTx: (Offset, LedgerEntry.Transaction)
  ): Future[Assertion] = {
    val (offset, tx) = offsetTx
    val maybeSubmitterInfo = submitterInfo(tx)
    val preparedInsert = prepareInsert(maybeSubmitterInfo, tx, CurrentOffset(offset))
    for {
      _ <- ledgerDao.storeTransactionEvents(preparedInsert)
      _ <- ledgerDao.storeTransactionState(preparedInsert)
      transactionTreeResponse <- ledgerDao.transactionsReader.lookupTransactionTreeById(
        transactionId = tx.transactionId,
        tx.actAs.toSet,
      )
      transactionResponse <- ledgerDao.transactionsReader.lookupFlatTransactionById(
        transactionId = tx.transactionId,
        tx.actAs.toSet,
      )
    } yield {
      transactionTreeResponse shouldBe None
      transactionResponse shouldBe None
    }
  }
}
