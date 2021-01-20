// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.EventId
import com.daml.lf.transaction.Node.NodeCreate
import com.daml.lf.value.Value.ContractId
import com.daml.platform.ApiOffset
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

trait JdbcIdempotentInsertionsSpec extends Inside with OptionValues with Matchers with LoneElement {
  self: AsyncFlatSpec with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (on PostgreSQL)"

  it should "allow idempotent transaction insertions" in {
    val key = "some-key"
    val create = txCreateContractWithKey(alice, key, Some("1337"))
    for {
      (offset, tx) <- store(create)
      _ <- store(create)
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
          created.contractKey.value.sum.text.value shouldBe key
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
}
