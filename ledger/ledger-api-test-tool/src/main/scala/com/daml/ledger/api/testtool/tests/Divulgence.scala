// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.daml.ledger.api.testtool.templates.{Divulgence1, Divulgence2}

final class Divulgence(session: LedgerSession) extends LedgerTestSuite(session) {

  private val transactionServiceDivulgence =
    LedgerTest("Divulged contracts should not be exposed by the transaction service") {
      implicit context =>
        for {
          Vector(alice, bob) <- allocateParties(2)
          divulgence1 <- Divulgence1(alice, alice)
          divulgence2 <- Divulgence2(bob, bob, alice)
          _ <- divulgence2.archive(alice, divulgence1)
          bobTransactions <- flatTransactions(bob)
          bobTrees <- transactionTrees(bob)
          aliceTransactions <- flatTransactions(alice)
        } yield {

          assert(
            bobTransactions.size == 1,
            s"Bob should see exactly one transaction but sees ${bobTransactions.size} instead")

          val events = bobTransactions.head.events
          assert(
            events.size == 1,
            s"The transaction should contain exactly one event but contains ${events.size} instead")

          val event = events.head.event
          assert(event.isCreated, "The transaction should contain a created event")

          val contractId = event.created.get.contractId
          assert(
            contractId == divulgence2.contractId,
            "The only visible event should be the creation of the second contract")

          assert(
            bobTrees.size == 2,
            s"Bob should see exactly two transaction trees but sees ${bobTrees.size} instead")

          val createDivulgence2Transaction = bobTrees(0)
          assert(
            createDivulgence2Transaction.rootEventIds.size == 1,
            s"The transaction that creates Divulgence2 should contain exactly one root event, but it contains ${createDivulgence2Transaction.rootEventIds.size} instead"
          )

          val createDivulgence2 =
            createDivulgence2Transaction.eventsById(createDivulgence2Transaction.rootEventIds(0))
          assert(
            createDivulgence2.kind.isCreated,
            s"Event expected to be a create"
          )

          assert(
            createDivulgence2.getCreated.contractId == divulgence2.contractId,
            s"The event where Divulgence2 is created should have the same contract identifier as the created contract"
          )

          val exerciseOnDivulgence2Transaction = bobTrees(1)
          assert(
            exerciseOnDivulgence2Transaction.rootEventIds.size == 1,
            s"The transaction where a choice is exercised on Divulgence2 should contain exactly one root event contains ${exerciseOnDivulgence2Transaction.rootEventIds.size} instead"
          )

          val exerciseOnDivulgence2 = exerciseOnDivulgence2Transaction.eventsById(
            exerciseOnDivulgence2Transaction.rootEventIds(0))
          assert(
            exerciseOnDivulgence2.kind.isExercised,
            s"Expected event to be an exercise"
          )

          assert(exerciseOnDivulgence2.getExercised.contractId == divulgence2.contractId)

          assert(exerciseOnDivulgence2.getExercised.childEventIds.size == 1)

          val exerciseOnDivulgence1 =
            exerciseOnDivulgence2Transaction.eventsById(
              exerciseOnDivulgence2.getExercised.childEventIds(0))

          assert(exerciseOnDivulgence1.kind.isExercised)

          assert(exerciseOnDivulgence1.getExercised.contractId == divulgence1.contractId)

          assert(exerciseOnDivulgence1.getExercised.childEventIds.isEmpty)

          assert(
            aliceTransactions.size == 3,
            s"Alice should see three transactions but sees ${aliceTransactions.size} instead")

          assert(aliceTransactions.head.events.size == 1)

          assert(aliceTransactions.head.events.head.event.isCreated)

          assert(
            aliceTransactions.head.events.head.event.created.get.contractId == divulgence1.contractId)

          assert(aliceTransactions.head.events.head.event.created.get.witnessParties == Seq(alice))
        }
    }

  private val activeContractServiceDivulgence = {
    LedgerTest("Divulged contracts should not be exposed by the active contract service") {
      implicit context =>
        for {
          Vector(alice, bob) <- allocateParties(2)
          divulgence1 <- Divulgence1(alice, alice)
          divulgence2 <- Divulgence2(bob, bob, alice)
          _ <- divulgence2.fetch(controller = alice, divulgence1)
          activeForBob <- activeContracts(bob)
          activeForAlice <- activeContracts(alice)
        } yield {
          assert(activeForBob.size == 1)
          assert(activeForBob.head.contractId == divulgence2.contractId)
          assert(activeForBob.head.witnessParties == Seq(bob))
          assert(activeForAlice.size == 2)
          assert(
            activeForAlice.map(_.contractId) == Seq(divulgence1.contractId, divulgence2.contractId))
          assert(activeForAlice(0).witnessParties == Seq(alice))
          assert(activeForAlice(1).witnessParties == Seq(alice))
        }
    }
  }

  override val tests: Vector[LedgerTest] = Vector(
    transactionServiceDivulgence,
    activeContractServiceDivulgence
  )

}
