// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.EventId
import com.digitalasset.platform.ApiOffset
import org.scalatest.{AsyncFlatSpec, Inside, LoneElement, Matchers, OptionValues}

private[dao] trait JdbcLedgerDaoTransactionsSpec extends OptionValues with Inside with LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (lookupFlatTransactionById)"

  it should "return nothing for a mismatching transaction id" in {
    for {
      (_, tx) <- storeCreateTransaction()
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(transactionId = "WRONG", Set(tx.submittingParty.get))
    } yield {
      result shouldBe None
    }
  }

  it should "return nothing for a mismatching party" in {
    for {
      (_, tx) <- storeCreateTransaction()
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, Set("WRONG"))
    } yield {
      result shouldBe None
    }
  }

  it should "return the expected flat transaction for a correct request (create)" in {
    for {
      (offset, tx) <- storeCreateTransaction()
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, Set(tx.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          transaction.commandId shouldBe tx.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.transactionId shouldBe tx.transactionId
          transaction.workflowId shouldBe tx.workflowId.getOrElse("")
          inside(transaction.events.loneElement.event.created) {
            case Some(created) =>
              val (eventId, createNode: NodeCreate.WithTxValue[AbsoluteContractId]) =
                tx.transaction.nodes.head
              created.eventId shouldBe eventId
              created.witnessParties should contain only tx.submittingParty.get
              created.agreementText.getOrElse("") shouldBe createNode.coinst.agreementText
              created.contractKey shouldBe None
              created.createArguments shouldNot be(None)
              created.signatories should contain theSameElementsAs createNode.signatories
              created.observers should contain theSameElementsAs createNode.stakeholders.diff(
                createNode.signatories)
              created.templateId shouldNot be(None)
          }
      }
    }
  }

  it should "return the expected flat transaction for a correct request (exercise)" in {
    for {
      (_, create) <- storeCreateTransaction()
      created <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(create.transactionId, Set(create.submittingParty.get))
      (offset, exercise) <- storeExerciseTransaction(
        AbsoluteContractId(created.get.transaction.get.events.head.event.created.get.contractId))
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(exercise.transactionId, Set(exercise.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          transaction.commandId shouldBe exercise.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.transactionId shouldBe exercise.transactionId
          transaction.workflowId shouldBe exercise.workflowId.getOrElse("")
          inside(transaction.events.loneElement.event.archived) {
            case Some(archived) =>
              val (eventId, exerciseNode: NodeExercises.WithTxValue[EventId, AbsoluteContractId]) =
                exercise.transaction.nodes.head
              archived.eventId shouldBe eventId
              archived.witnessParties should contain only exercise.submittingParty.get
              archived.contractId shouldBe exerciseNode.targetCoid.coid
              archived.templateId shouldNot be(None)
          }
      }
    }
  }

  it should "hide events on transient contracts to the original submitter" in {
    for {
      (offset, tx) <- storeFullyTransientTransaction()
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, Set(tx.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          transaction.commandId shouldBe tx.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.transactionId shouldBe tx.transactionId
          transaction.workflowId shouldBe tx.workflowId.getOrElse("")
          transaction.events shouldBe Seq.empty
      }
    }
  }

  it should "hide a full transaction if all contracts are transient and the request does not come from the original submitter" in {
    for {
      (_, tx) <- storeFullyTransientTransaction()
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, Set(bob))
    } yield {
      result shouldBe None
    }
  }

}
