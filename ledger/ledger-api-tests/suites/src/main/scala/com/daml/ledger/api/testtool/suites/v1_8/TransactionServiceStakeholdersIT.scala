// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.test.model.Test._
import scalaz.Tag

import scala.collection.immutable.Seq

class TransactionServiceStakeholdersIT extends LedgerTestSuite {
  test("TXStakeholders", "Expose the correct stakeholders", allocate(SingleParty, SingleParty))(
    implicit ec => {
      case Participants(Participant(alpha @ _, receiver), Participant(beta, giver)) =>
        for {
          _ <- beta.create(giver, CallablePayout(giver, receiver))
          transactions <- beta.flatTransactions(giver, receiver)
        } yield {
          val contract = assertSingleton("Stakeholders", transactions.flatMap(createdEvents))
          assertEquals("Signatories", contract.signatories, Seq(Tag.unwrap(giver)))
          assertEquals("Observers", contract.observers, Seq(Tag.unwrap(receiver)))
        }
    }
  )

  test(
    "TXnoSignatoryObservers",
    "transactions' created events should not return overlapping signatories and observers",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- ledger.create(alice, WithObservers(alice, Seq(alice, bob)))
      flat <- ledger.flatTransactions(alice)
      Seq(flatTx) = flat
      Seq(flatWo) = createdEvents(flatTx)
      tree <- ledger.transactionTrees(alice)
      Seq(treeTx) = tree
      Seq(treeWo) = createdEvents(treeTx)
    } yield {
      assert(
        flatWo.observers == Seq(bob),
        s"Expected observers to only contain $bob, but received ${flatWo.observers}",
      )
      assert(
        treeWo.observers == Seq(bob),
        s"Expected observers to only contain $bob, but received ${treeWo.observers}",
      )
    }
  })

  test(
    "TXTransientObservableSubmitter",
    "transactions with transient only events should be visible to submitting party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    // Create command with transient contract
    val createAndExercise = Dummy(party).createAnd.exerciseArchive().command
    for {
      _ <- ledger.submitAndWait(ledger.submitAndWaitRequest(party, createAndExercise))

      emptyFlatTx <- ledger.flatTransactions(party)
      emptyFlatTxByTemplateId <- ledger.flatTransactionsByTemplateId(Dummy.id, party)
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
      WithObservers(submitter, List(observer)).createAnd.exerciseArchive().command
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
      emptyFlatTxByTemplateId <- ledger.flatTransactionsByTemplateId(WithObservers.id, observer)
    } yield {
      assert(emptyFlatTx.isEmpty, s"No transaction expected but got $emptyFlatTx instead")
      assert(
        emptyFlatTxByTemplateId.isEmpty,
        s"No transaction expected but got $emptyFlatTxByTemplateId instead",
      )
    }
  })
}
