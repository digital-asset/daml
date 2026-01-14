// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.test.java.model.test.{CallablePayout, Dummy, WithObservers}
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.*

class TransactionServiceStakeholdersIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test("TXStakeholders", "Expose the correct stakeholders", allocate(SingleParty, SingleParty))(
    implicit ec => {
      case Participants(Participant(alpha @ _, Seq(receiver)), Participant(beta, Seq(giver))) =>
        for {
          _ <- beta.create(giver, new CallablePayout(giver, receiver))
          transactions <- beta.transactions(AcsDelta, giver, receiver)
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
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      _ <- ledger.create(alice, new WithObservers(alice, Seq(alice, bob).map(_.getValue).asJava))
      acsDeltaTx <- ledger.transactions(AcsDelta, alice).map(_.headOption.value)
      acsDeltaWo = createdEvents(acsDeltaTx).headOption.value
      ledgerEffectsTx <- ledger.transactions(LedgerEffects, alice).map(_.headOption.value)
      ledgerEffectsWo = createdEvents(ledgerEffectsTx).headOption.value
    } yield {
      assert(
        acsDeltaWo.observers == Seq(bob.getValue),
        s"Expected observers to only contain ${bob.getValue}, but received ${acsDeltaWo.observers}",
      )
      assert(
        ledgerEffectsWo.observers == Seq(bob.getValue),
        s"Expected observers to only contain ${bob.getValue}, but received ${ledgerEffectsWo.observers}",
      )
    }
  })

  test(
    "TXTransientObservableSubmitter",
    "transactions containing only transient evens should not be visible to submitting party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    // Create command with transient contract
    val createAndExercise = new Dummy(party).createAnd.exerciseArchive().commands
    for {
      _ <- ledger.submitAndWait(ledger.submitAndWaitRequest(party, createAndExercise))

      emptyFlatTx <- ledger.transactions(AcsDelta, party)
      emptyFlatTxByTemplateId <- ledger
        .transactionsByTemplateId(Dummy.TEMPLATE_ID, Some(Seq(party)))
    } yield {
      assert(
        emptyFlatTx.isEmpty,
        s"Expected no flat transaction, but received $emptyFlatTx",
      )
      assert(
        emptyFlatTxByTemplateId.isEmpty,
        s"Expected no transaction for Dummy, but received $emptyFlatTxByTemplateId",
      )
    }
  })

  test(
    "TXTransientNotObservableNoSubmitters",
    "transactions with transient only events should not be visible if requester is not a submitting party",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(submitter, observer))) =>
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
      emptyFlatTx <- ledger.transactions(AcsDelta, observer)
      emptyFlatTxByTemplateId <- ledger.transactionsByTemplateId(
        WithObservers.TEMPLATE_ID,
        Some(Seq(observer)),
      )
    } yield {
      assert(emptyFlatTx.isEmpty, s"No transaction expected but got $emptyFlatTx instead")
      assert(
        emptyFlatTxByTemplateId.isEmpty,
        s"No transaction expected but got $emptyFlatTxByTemplateId instead",
      )
    }
  })

  test(
    "TXTransientSubmitAndWaitForTransaction",
    "transactions containing only transient evens and submitted via submitAndWaitForTransaction should not contain any events",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    // Create command with transient contract
    val createAndExercise = new Dummy(party).createAnd.exerciseArchive().commands
    for {
      response <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(party, createAndExercise)
      )
    } yield {
      val tx = assertSingleton(
        s"Expected a transaction but did not received one",
        response.transaction.toList,
      )
      assert(
        tx.events.isEmpty,
        s"Expected no events, but received ${tx.events}",
      )
    }
  })

}
