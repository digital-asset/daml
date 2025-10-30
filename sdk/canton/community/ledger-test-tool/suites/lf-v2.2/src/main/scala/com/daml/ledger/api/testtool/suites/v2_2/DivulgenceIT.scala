// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionOps.*
import com.daml.ledger.test.java.model.test.{Divulgence1, Divulgence2}
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.platform.store.utils.EventOps.EventOps

final class DivulgenceIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "DivulgenceTx",
    "Divulged contracts should not be exposed by the transaction service",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      divulgence1 <- ledger.create(alice, new Divulgence1(alice))
      divulgence2 <- ledger.create(bob, new Divulgence2(bob, alice))
      _ <- ledger.exercise(alice, divulgence2.exerciseDivulgence2Archive(divulgence1))
      bobTransactions <- ledger.transactions(AcsDelta, bob)
      bobTransactionsLedgerEffects <- ledger.transactions(LedgerEffects, bob)
      transactionsForBoth <- ledger.transactions(AcsDelta, alice, bob)
    } yield {

      // Inspecting the acs delta transaction stream as seen by Bob

      // We expect only one transaction containing only one create event for Divulgence2.
      // We expect to _not_ see the create or archive for Divulgence1, even if Divulgence1 was divulged
      // to Bob, and even if the exercise is visible to Bob in the ledger effects.

      assert(
        bobTransactions.sizeIs == 1,
        s"${bob.getValue} should see exactly one transaction but sees ${bobTransactions.size} instead",
      )

      val events = bobTransactions.head.events
      assert(
        events.sizeIs == 1,
        s"The transaction should contain exactly one event but contains ${events.size} instead",
      )

      val event = events.head.event
      assert(
        event.isCreated,
        s"The only event in the transaction was expected to be a created event",
      )

      val contractId = event.created.get.contractId
      assert(
        contractId == divulgence2.contractId,
        s"The only visible event should be the creation of the second contract (expected $divulgence2, got $contractId instead)",
      )

      // Inspecting the ledger effects as seen by Bob

      // then what we expect for Bob's ledger effects transactions. note that here we witness the exercise that
      // caused the archive of div1Cid, even if we did _not_ see the archive event in the acs delta transaction
      // stream above

      // We expect to see two transactions: one for the second create and one for the exercise.

      assert(
        bobTransactionsLedgerEffects.sizeIs == 2,
        s"$bob should see exactly two transactions but sees ${bobTransactionsLedgerEffects.size} instead",
      )

      val createDivulgence2Transaction = bobTransactionsLedgerEffects(0)
      assert(
        createDivulgence2Transaction.rootNodeIds().sizeIs == 1,
        s"The transaction that creates Divulgence2 should contain exactly one root event, but it contains ${createDivulgence2Transaction.rootNodeIds().size} instead",
      )

      val createDivulgence2 =
        createDivulgence2Transaction.events.head
      assert(
        createDivulgence2.event.isCreated,
        s"Event expected to be a create",
      )

      val createDivulgence2ContractId = createDivulgence2.getCreated.contractId
      assert(
        createDivulgence2ContractId == divulgence2.contractId,
        s"The event where Divulgence2 is created should have the same contract identifier as the created contract (expected $divulgence2, got $createDivulgence2ContractId instead)",
      )

      val exerciseOnDivulgence2Transaction = bobTransactionsLedgerEffects(1)
      assert(
        exerciseOnDivulgence2Transaction.rootNodeIds().sizeIs == 1,
        s"The transaction where a choice is exercised on Divulgence2 should contain exactly one root event contains ${exerciseOnDivulgence2Transaction.rootNodeIds().size} instead",
      )

      val exerciseOnDivulgence2 = exerciseOnDivulgence2Transaction.events.head
      assert(
        exerciseOnDivulgence2.event.isExercised,
        s"Expected event to be an exercise",
      )

      assert(exerciseOnDivulgence2.getExercised.contractId == divulgence2.contractId)

      assert(
        exerciseOnDivulgence2.getExercised.lastDescendantNodeId - exerciseOnDivulgence2.getExercised.nodeId == 1
      )

      val exerciseOnDivulgence1 =
        exerciseOnDivulgence2Transaction.events
          .find(
            _.nodeId == exerciseOnDivulgence2.getExercised.lastDescendantNodeId
          )
          .get

      assert(exerciseOnDivulgence1.event.isExercised)

      assert(exerciseOnDivulgence1.getExercised.contractId == divulgence1.contractId)

      assert(
        exerciseOnDivulgence1.getExercised.lastDescendantNodeId == exerciseOnDivulgence1.getExercised.nodeId
      )

      // Alice should see:
      // - create Divulgence1
      // - create Divulgence2
      // - archive Divulgence1
      // Note that we do _not_ see the exercise of Divulgence2 because it is nonconsuming.

      assert(
        transactionsForBoth.sizeIs == 3,
        s"Filtering for both $alice and $bob should result in three transactions seen but ${transactionsForBoth.size} are seen instead",
      )

      val firstTransactionForBoth = transactionsForBoth.head
      assert(
        firstTransactionForBoth.events.sizeIs == 1,
        s"The first transaction seen by filtering for both $alice and $bob should contain exactly one event but it contains ${firstTransactionForBoth.events.size} events instead",
      )

      val firstEventForBoth = transactionsForBoth.head.events.head.event
      assert(
        firstEventForBoth.isCreated,
        s"The first event seen by filtering for both $alice and $bob was expected to be a creation",
      )

      val firstCreationForBoth = firstEventForBoth.created.get
      assert(
        firstCreationForBoth.contractId == divulgence1.contractId,
        s"The creation seen by filtering for both $alice and $bob was expected to be $divulgence1 but is ${firstCreationForBoth.contractId} instead",
      )

      assert(
        firstCreationForBoth.witnessParties == Seq(alice.getValue),
        s"The creation seen by filtering for both $alice and $bob was expected to be witnessed by $alice but is instead ${firstCreationForBoth.witnessParties}",
      )
    }
  })

  test(
    "ImmediateDivulgenceTx",
    "Immediately divulged contracts (created events) should not be exposed by the transaction service",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      divulgence2 <- ledger.create(bob, new Divulgence2(bob, alice))
      _ <- ledger.exercise(alice, divulgence2.exerciseDivulgence2ImmediateDivulge())
      bobTransactions <- ledger.transactions(AcsDelta, bob)
      bobTransactionsLedgerEffects <- ledger.transactions(LedgerEffects, bob)
    } yield {

      // Inspecting the acs delta transaction stream as seen by Bob

      // We expect only one transaction containing only one create event for Divulgence2.
      // We expect to _not_ see the create for Divulgence1, even if Divulgence1 was divulged
      // to Bob, and even if the exercise is visible to Bob in the ledger effects.

      assert(
        bobTransactions.sizeIs == 1,
        s"${bob.getValue} should see exactly one transaction but sees ${bobTransactions.size} instead",
      )

      val events = bobTransactions.head.events
      assert(
        events.sizeIs == 1,
        s"The transaction should contain exactly one event but contains ${events.size} instead",
      )

      val event = events.head.event
      assert(
        event.isCreated,
        s"The only event in the transaction was expected to be a created event",
      )

      val contractId = event.created.get.contractId
      assert(
        contractId == divulgence2.contractId,
        s"The only visible event should be the creation of the Divulgence2 contract (expected $divulgence2, got $contractId instead)",
      )

      // Inspecting the ledger effects as seen by Bob

      // then what we expect for Bob's ledger effects transactions. note that here we witness the exercise that
      // caused the create of div1Cid, even if we did _not_ see the create event in the acs delta transaction
      // stream above

      // We expect to see two transactions: one for the first create and one for the exercise.

      assert(
        bobTransactionsLedgerEffects.sizeIs == 2,
        s"$bob should see exactly two transactions but sees ${bobTransactionsLedgerEffects.size} instead",
      )

      val createDivulgence2Transaction = bobTransactionsLedgerEffects(0)
      assert(
        createDivulgence2Transaction.rootNodeIds().sizeIs == 1,
        s"The transaction that creates Divulgence2 should contain exactly one root event, but it contains ${createDivulgence2Transaction.rootNodeIds().size} instead",
      )

      val createDivulgence2 =
        createDivulgence2Transaction.events.head
      assert(
        createDivulgence2.event.isCreated,
        s"Event expected to be a create",
      )

      val createDivulgence2ContractId = createDivulgence2.getCreated.contractId
      assert(
        createDivulgence2ContractId == divulgence2.contractId,
        s"The event where Divulgence2 is created should have the same contract identifier as the created contract (expected $divulgence2, got $createDivulgence2ContractId instead)",
      )

      val exerciseOnDivulgence2Transaction = bobTransactionsLedgerEffects(1)
      assert(
        exerciseOnDivulgence2Transaction.rootNodeIds().sizeIs == 1,
        s"The transaction where a choice is exercised on Divulgence2 should contain exactly one root event contains ${exerciseOnDivulgence2Transaction.rootNodeIds().size} instead",
      )

      val exerciseOnDivulgence2 = exerciseOnDivulgence2Transaction.events.head
      assert(
        exerciseOnDivulgence2Transaction.events.sizeIs == 2,
        "The transaction should contain exactly two events, one for the exercise and one for the create",
      )
      assert(
        exerciseOnDivulgence2.event.isExercised,
        s"Expected event to be an exercise",
      )
      assertAcsDelta(
        exerciseOnDivulgence2Transaction.events,
        acsDelta = false,
        "None of the events in the transaction should have acs_delta set, as this is a non-consuming exercise and an immediately divulged create",
      )

      assert(exerciseOnDivulgence2.getExercised.contractId == divulgence2.contractId)

      assert(
        exerciseOnDivulgence2.getExercised.lastDescendantNodeId - exerciseOnDivulgence2.getExercised.nodeId == 1,
        "The last descendant node id should be one more than the node id, since it contains the create event",
      )
    }
  })

  test(
    "DivulgenceAcs",
    "Divulged contracts should not be exposed by the active contract service",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      divulgence1 <- ledger.create(alice, new Divulgence1(alice))
      divulgence2 <- ledger.create(bob, new Divulgence2(bob, alice))
      _ <- ledger.exercise(alice, divulgence2.exerciseDivulgence2Fetch(divulgence1))
      activeForBobOnly <- ledger.activeContracts(Some(Seq(bob)))
      activeForBoth <- ledger.activeContracts(Some(Seq(alice, bob)))
    } yield {

      // Bob only sees Divulgence2
      assert(
        activeForBobOnly.sizeIs == 1,
        s"$bob should see only one active contract but sees ${activeForBobOnly.size} instead",
      )
      assert(
        activeForBobOnly.head.contractId == divulgence2.contractId,
        s"$bob should see $divulgence2 but sees ${activeForBobOnly.head.contractId} instead",
      )

      // Since we're filtering for Bob only Bob will be the only reported witness even if Alice sees the contract
      assert(
        activeForBobOnly.head.witnessParties == Seq(bob.getValue),
        s"The witness parties as seen by $bob should only include him but it is instead ${activeForBobOnly.head.witnessParties}",
      )

      // Alice sees both
      assert(
        activeForBoth.sizeIs == 2,
        s"The active contracts as seen by $alice and $bob should be two but are ${activeForBoth.size} instead",
      )
      val divulgence1ContractId = divulgence1.contractId
      val divulgence2ContractId = divulgence2.contractId
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
        divulgence1Witnesses == Seq(alice.getValue),
        s"The witness parties of the first contract should only include $alice but it is instead $divulgence1Witnesses ($bob)",
      )
      assert(
        divulgence2Witnesses == Seq(alice, bob).map(_.getValue).sorted,
        s"The witness parties of the second contract should include $alice and $bob but it is instead $divulgence2Witnesses",
      )
    }
  })

}
