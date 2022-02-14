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
}
