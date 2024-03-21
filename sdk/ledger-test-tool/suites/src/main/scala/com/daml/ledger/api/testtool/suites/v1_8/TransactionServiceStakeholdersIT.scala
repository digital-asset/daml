// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.test.java.model.test._

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class TransactionServiceStakeholdersIT extends LedgerTestSuite {
  import CompanionImplicits._

  test("TXStakeholders", "Expose the correct stakeholders", allocate(SingleParty, SingleParty))(
    implicit ec => {
      case Participants(Participant(alpha @ _, receiver), Participant(beta, giver)) =>
        for {
          _ <- beta.create(giver, new CallablePayout(giver, receiver))
          transactions <- beta.flatTransactions(giver, receiver)
        } yield {
          val contract = assertSingleton("Stakeholders", transactions.flatMap(createdEvents))
          assertEquals("Signatories", contract.signatories, Seq(giver.getValue))
          assertEquals("Observers", contract.observers, Seq(receiver.getValue))
        }
    }
  )

  test(
    "TXnoSignatoryObservers",
    "transactions' created events should not return overlapping signatories and observers",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- ledger.create(alice, new WithObservers(alice, Seq(alice, bob).map(_.getValue).asJava))
      flatTx <- ledger.flatTransactions(alice).flatMap(fs => Future(fs.head))
      flatWo <- Future(createdEvents(flatTx).head)
      treeTx <- ledger.transactionTrees(alice).flatMap(fs => Future(fs.head))
      treeWo <- Future(createdEvents(treeTx).head)
    } yield {
      assert(
        flatWo.observers == Seq(bob.getValue),
        s"Expected observers to only contain ${bob.getValue}, but received ${flatWo.observers}",
      )
      assert(
        treeWo.observers == Seq(bob.getValue),
        s"Expected observers to only contain ${bob.getValue}, but received ${treeWo.observers}",
      )
    }
  })

  test(
    "TXTransientObservableSubmitter",
    "transactions with transient only events should be visible to submitting party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    // Create command with transient contract
    val createAndExercise = new Dummy(party).createAnd.exerciseArchive().commands
    for {
      _ <- ledger.submitAndWait(ledger.submitAndWaitRequest(party, createAndExercise))

      emptyFlatTx <- ledger.flatTransactions(party)
      emptyFlatTxByTemplateId <- ledger.flatTransactionsByTemplateId(Dummy.TEMPLATE_ID, party)
    } yield {
      assert(
        emptyFlatTx.length == 1,
        s"Expected 1 flat transaction, but received $emptyFlatTx",
      )
      assert(
        emptyFlatTx.head.events.isEmpty,
        s"Expected empty transaction events",
      )
      assert(
        emptyFlatTxByTemplateId.length == 1,
        s"Expected 1 flat transaction for Dummy, but received $emptyFlatTxByTemplateId",
      )
      assert(
        emptyFlatTxByTemplateId.head.events.isEmpty,
        s"Expected empty transaction events, but got ${emptyFlatTxByTemplateId.head.events}",
      )
    }
  })

  test(
    "TXTransientNotObservableNoSubmitters",
    "transactions with transient only events should not be visible if requester is not a submitting party",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, submitter, observer)) =>
    // Create command with transient contract
    val createAndExerciseWithObservers =
      new WithObservers(submitter, List(observer.getValue).asJava).createAnd
        .exerciseArchive()
        .commands
    for {
      // The in-memory fan-out serves at least N-1 transaction responses from a specific query window
      // Then, submit 2 requests to ensure that a transaction from the in-memory fan-out would be forwarded.
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(submitter, createAndExerciseWithObservers)
      )
      _ <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(submitter, createAndExerciseWithObservers)
      )
      // `observer` is just a stakeholder on the contract created by `submitter`
      // but it should not see the completion/flat transaction if it has only transient events.
      emptyFlatTx <- ledger.flatTransactions(observer)
      emptyFlatTxByTemplateId <- ledger.flatTransactionsByTemplateId(
        WithObservers.TEMPLATE_ID,
        observer,
      )
    } yield {
      assert(emptyFlatTx.isEmpty, s"No transaction expected but got $emptyFlatTx instead")
      assert(
        emptyFlatTxByTemplateId.isEmpty,
        s"No transaction expected but got $emptyFlatTxByTemplateId instead",
      )
    }
  })
}
