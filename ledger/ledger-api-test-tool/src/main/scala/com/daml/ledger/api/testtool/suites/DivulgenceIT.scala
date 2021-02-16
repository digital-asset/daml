// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.waitForContract
import com.daml.ledger.test.model.Test.Divulgence2._
import com.daml.ledger.test.model.Test.Proposal._
import com.daml.ledger.test.model.Test.{Asset, Divulgence1, Divulgence2, Proposal}
import scalaz.Tag

final class DivulgenceIT extends LedgerTestSuite {

  // TODO this one is a flake, at daml-on-sql it fails constantly in orchestration, but never in isolation, on ledger-on-memory it fails like 5% of the runs
  // TODO for PoC purposes: suggestion to omit this failure, since probably no relation to broken functionality
  test(
    "DivulgenceTx",
    "Divulged contracts should not be exposed by the transaction service",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      divulgence1 <- ledger.create(alice, Divulgence1(alice))
      divulgence2 <- ledger.create(bob, Divulgence2(bob, alice))
      _ <- ledger.exercise(alice, divulgence2.exerciseDivulgence2Archive(_, divulgence1))
      bobTransactions <- ledger.flatTransactions(bob)
      bobTrees <- ledger.transactionTrees(bob)
      transactionsForBoth <- ledger.flatTransactions(alice, bob)
    } yield {

      // Inspecting the flat transaction stream as seen by Bob

      // We expect only one transaction containing only one create event for Divulgence2.
      // We expect to _not_ see the create or archive for Divulgence1, even if Divulgence1 was divulged
      // to Bob, and even if the exercise is visible to Bob in the transaction trees.

      assert(
        bobTransactions.size == 1,
        s"$bob should see exactly one transaction but sees ${bobTransactions.size} instead",
      )

      val events = bobTransactions.head.events
      assert(
        events.size == 1,
        s"The transaction should contain exactly one event but contains ${events.size} instead",
      )

      val event = events.head.event
      assert(
        event.isCreated,
        s"The only event in the transaction was expected to be a created event",
      )

      val contractId = event.created.get.contractId
      assert(
        contractId == divulgence2,
        s"The only visible event should be the creation of the second contract (expected $divulgence2, got $contractId instead)",
      )

      // Inspecting the transaction trees as seen by Bob

      // then what we expect for Bob's tree transactions. note that here we witness the exercise that
      // caused the archive of div1Cid, even if we did _not_ see the archive event in the flat transaction
      // stream above

      // We expect to see two transactions: one for the second create and one for the exercise.

      assert(
        bobTrees.size == 2,
        s"$bob should see exactly two transaction trees but sees ${bobTrees.size} instead",
      )

      val createDivulgence2Transaction = bobTrees(0)
      assert(
        createDivulgence2Transaction.rootEventIds.size == 1,
        s"The transaction that creates Divulgence2 should contain exactly one root event, but it contains ${createDivulgence2Transaction.rootEventIds.size} instead",
      )

      val createDivulgence2 =
        createDivulgence2Transaction.eventsById(createDivulgence2Transaction.rootEventIds.head)
      assert(
        createDivulgence2.kind.isCreated,
        s"Event expected to be a create",
      )

      val createDivulgence2ContractId = createDivulgence2.getCreated.contractId
      assert(
        createDivulgence2ContractId == divulgence2,
        s"The event where Divulgence2 is created should have the same contract identifier as the created contract (expected $divulgence2, got $createDivulgence2ContractId instead)",
      )

      val exerciseOnDivulgence2Transaction = bobTrees(1)
      assert(
        exerciseOnDivulgence2Transaction.rootEventIds.size == 1,
        s"The transaction where a choice is exercised on Divulgence2 should contain exactly one root event contains ${exerciseOnDivulgence2Transaction.rootEventIds.size} instead",
      )

      val exerciseOnDivulgence2 = exerciseOnDivulgence2Transaction.eventsById(
        exerciseOnDivulgence2Transaction.rootEventIds.head
      )
      assert(
        exerciseOnDivulgence2.kind.isExercised,
        s"Expected event to be an exercise",
      )

      assert(exerciseOnDivulgence2.getExercised.contractId == divulgence2)

      assert(exerciseOnDivulgence2.getExercised.childEventIds.size == 1)

      val exerciseOnDivulgence1 =
        exerciseOnDivulgence2Transaction.eventsById(
          exerciseOnDivulgence2.getExercised.childEventIds.head
        )

      assert(exerciseOnDivulgence1.kind.isExercised)

      assert(exerciseOnDivulgence1.getExercised.contractId == divulgence1)

      assert(exerciseOnDivulgence1.getExercised.childEventIds.isEmpty)

      // Alice should see:
      // - create Divulgence1
      // - create Divulgence2
      // - archive Divulgence1
      // Note that we do _not_ see the exercise of Divulgence2 because it is nonconsuming.

      assert(
        transactionsForBoth.size == 3,
        s"Filtering for both $alice and $bob should result in three transactions seen but ${transactionsForBoth.size} are seen instead",
      )

      val firstTransactionForBoth = transactionsForBoth.head
      assert(
        firstTransactionForBoth.events.size == 1,
        s"The first transaction seen by filtering for both $alice and $bob should contain exactly one event but it contains ${firstTransactionForBoth.events.size} events instead",
      )

      val firstEventForBoth = transactionsForBoth.head.events.head.event
      assert(
        firstEventForBoth.isCreated,
        s"The first event seen by filtering for both $alice and $bob was expected to be a creation",
      )

      val firstCreationForBoth = firstEventForBoth.created.get
      assert(
        firstCreationForBoth.contractId == divulgence1,
        s"The creation seen by filtering for both $alice and $bob was expected to be $divulgence1 but is ${firstCreationForBoth.contractId} instead",
      )

      assert(
        firstCreationForBoth.witnessParties == Seq(alice),
        s"The creation seen by filtering for both $alice and $bob was expected to be witnessed by $alice but is instead ${firstCreationForBoth.witnessParties}",
      )
    }
  })

  test(
    "DivulgenceAcs",
    "Divulged contracts should not be exposed by the active contract service",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      divulgence1 <- ledger.create(alice, Divulgence1(alice))
      divulgence2 <- ledger.create(bob, Divulgence2(bob, alice))
      _ <- ledger.exercise(alice, divulgence2.exerciseDivulgence2Fetch(_, divulgence1))
      activeForBobOnly <- ledger.activeContracts(bob)
      activeForBoth <- ledger.activeContracts(alice, bob)
    } yield {

      // Bob only sees Divulgence2
      assert(
        activeForBobOnly.size == 1,
        s"$bob should see only one active contract but sees ${activeForBobOnly.size} instead",
      )
      assert(
        activeForBobOnly.head.contractId == divulgence2,
        s"$bob should see $divulgence2 but sees ${activeForBobOnly.head.contractId} instead",
      )

      // Since we're filtering for Bob only Bob will be the only reported witness even if Alice sees the contract
      assert(
        activeForBobOnly.head.witnessParties == Seq(bob),
        s"The witness parties as seen by $bob should only include him but it is instead ${activeForBobOnly.head.witnessParties}",
      )

      // Alice sees both
      assert(
        activeForBoth.size == 2,
        s"The active contracts as seen by $alice and $bob should be two but are ${activeForBoth.size} instead",
      )
      val divulgence1ContractId = Tag.unwrap(divulgence1)
      val divulgence2ContractId = Tag.unwrap(divulgence2)
      val activeForBothContractIds = activeForBoth.map(_.contractId).sorted
      val expectedContractIds = Seq(divulgence1ContractId, divulgence2ContractId).sorted
      assert(
        activeForBothContractIds == expectedContractIds,
        s"$divulgence1 and $divulgence2 are expected to be seen when filtering for $alice and $bob but instead the following contract identifiers are seen: $activeForBothContractIds",
      )
      val divulgence1Witnesses =
        activeForBoth.find(_.contractId == divulgence1ContractId).get.witnessParties.sorted
      val divulgence2Witnesses =
        activeForBoth.find(_.contractId == divulgence2ContractId).get.witnessParties.sorted
      assert(
        divulgence1Witnesses == Seq(alice),
        s"The witness parties of the first contract should only include $alice but it is instead $divulgence1Witnesses ($bob)",
      )
      assert(
        divulgence2Witnesses == Tag.unsubst(Seq(alice, bob)).sorted,
        s"The witness parties of the second contract should include $alice and $bob but it is instead $divulgence2Witnesses",
      )
    }
  })

  test(
    "DivulgenceKeys",
    "Divulgence should behave as expected in a workflow involving keys",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, proposer), Participant(beta, owner)) =>
    for {
      offer <- alpha.create(proposer, Proposal(from = proposer, to = owner))
      asset <- beta.create(owner, Asset(issuer = owner, owner = owner))
      _ <- waitForContract(beta, owner, offer)
      _ <- beta.exercise(owner, offer.exerciseProposalAccept(_, asset))
    } yield {
      // nothing to test, if the workflow ends successfully the test is considered successful
    }
  })
}
