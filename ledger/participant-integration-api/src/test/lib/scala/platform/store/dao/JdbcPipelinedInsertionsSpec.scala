// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.EventId
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.transaction.Node.NodeCreate
import com.daml.lf.value.Value.ContractId
import com.daml.platform.ApiOffset
import com.daml.platform.indexer.CurrentOffset
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside, LoneElement, OptionValues}

import scala.concurrent.Future

trait JdbcPipelinedInsertionsSpec extends Inside with OptionValues with Matchers with LoneElement {
  self: AsyncFlatSpec with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (on PostgreSQL)"

  // TODO ignoring this test: this idempotent re-ingestion approach is backed by unique index on event_id
  // TODO with future ingestion strategies this might be not necessary
  // TODO current PoC schema does not support this DB level uniqueness checks
  // TODO cc @simon@
  ignore should "allow idempotent transaction insertions" in {
    val key = "some-key"
    val create @ (offset, tx) = txCreateContractWithKey(alice, key, Some("1337"))
    val maybeSubmitterInfo = submitterInfo(tx)
    val preparedInsert = prepareInsert(maybeSubmitterInfo, tx, CurrentOffset(offset))
    for {
      _ <- ledgerDao.storeTransactionEvents(preparedInsert)
      // Assume the indexer restarts after events insertion
      _ <- ledgerDao.storeTransactionEvents(preparedInsert)
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
