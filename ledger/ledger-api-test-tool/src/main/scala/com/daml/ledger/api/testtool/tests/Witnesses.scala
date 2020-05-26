// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.daml.ledger.test_stable.Test.DivulgeWitnesses._
import com.daml.ledger.test_stable.Test.Witnesses._
import com.daml.ledger.test_stable.Test.{DivulgeWitnesses, Witnesses => WitnessesTemplate}
import scalaz.Tag

final class Witnesses(session: LedgerSession) extends LedgerTestSuite(session) {
  test("RespectDisclosureRules", "The ledger should respect disclosure rules", allocate(Parties(3))) {
    case Participants(Participant(ledger, alice, bob, charlie)) =>
      for {
        // Create the Witnesses contract as Alice and get the resulting transaction as seen by all parties
        (witnessesTransactionId, witnesses) <- ledger.createAndGetTransactionId(
          alice,
          WitnessesTemplate(alice, bob, charlie),
        )
        witnessesTransaction <- ledger.transactionTreeById(
          witnessesTransactionId,
          alice,
          bob,
          charlie,
        )

        // Charlie is not a stakeholder of Witnesses and thus cannot see any such contract unless divulged.
        // Such contract is divulged by creating a DivulgeWitness with Charlie as a signatory and exercising
        // a choice as Alice that causes divulgence (in this case, the Witnesses instance previously
        // created is fetched as part of the transaction).
        divulgeWitness <- ledger.create(charlie, DivulgeWitnesses(alice, charlie))
        _ <- ledger.exercise(alice, divulgeWitness.exerciseDivulge(_, witnesses))

        // A non-consuming choice is exercised with the expectation
        // that Charlie is now able to exercise a choice on the divulged contract
        // The tree is fetched from the identifier to ensure we get the witnesses as seen by all parties
        nonConsuming <- ledger.exercise(charlie, witnesses.exerciseWitnessesNonConsumingChoice)
        nonConsumingTree <- ledger.transactionTreeById(
          nonConsuming.transactionId,
          alice,
          bob,
          charlie,
        )

        // A consuming choice is exercised with the expectation
        // that Charlie is now able to exercise a choice on the divulged contract
        // The tree is fetched from the identifier to ensure we get the witnesses as seen by all parties
        consuming <- ledger.exercise(charlie, witnesses.exerciseWitnessesChoice)
        consumingTree <- ledger.transactionTreeById(consuming.transactionId, alice, bob, charlie)
      } yield {

        assert(
          witnessesTransaction.eventsById.size == 1,
          s"The transaction for creating the Witness contract should only contain a single event, but has ${witnessesTransaction.eventsById.size}",
        )
        val (_, creationEvent) = witnessesTransaction.eventsById.head
        assert(
          creationEvent.kind.isCreated,
          s"The event in the transaction for creating the Witness should be a CreatedEvent, but was ${creationEvent.kind}",
        )

        val expectedWitnessesOfCreation = Tag.unsubst(Seq(alice, bob)).sorted
        assert(
          creationEvent.getCreated.witnessParties.sorted == expectedWitnessesOfCreation,
          s"The parties for witnessing the CreatedEvent should be ${expectedWitnessesOfCreation}, but were ${creationEvent.getCreated.witnessParties}",
        )
        assert(
          nonConsumingTree.eventsById.size == 1,
          s"The transaction for exercising the non-consuming choice should only contain a single event, but has ${nonConsumingTree.eventsById.size}",
        )
        val (_, nonConsumingEvent) = nonConsumingTree.eventsById.head
        assert(
          nonConsumingEvent.kind.isExercised,
          s"The event in the transaction for exercising the non-consuming choice should be an ExercisedEvent, but was ${nonConsumingEvent.kind}",
        )

        val expectedWitnessesOfNonConsumingChoice = Tag.unsubst(Seq(alice, charlie)).sorted
        assert(
          nonConsumingEvent.getExercised.witnessParties.sorted == expectedWitnessesOfNonConsumingChoice,
          s"The parties for witnessing the non-consuming ExercisedEvent should be ${expectedWitnessesOfNonConsumingChoice}, but were ${nonConsumingEvent.getCreated.witnessParties}",
        )
        assert(
          consumingTree.eventsById.size == 1,
          s"The transaction for exercising the consuming choice should only contain a single event, but has ${consumingTree.eventsById.size}",
        )

        val (_, consumingEvent) = consumingTree.eventsById.head
        assert(
          consumingEvent.kind.isExercised,
          s"The event in the transaction for exercising the consuming choice should be an ExercisedEvent, but was ${consumingEvent.kind}",
        )
        val expectedWitnessesOfConsumingChoice = Tag.unsubst(Seq(alice, bob, charlie)).sorted
        assert(
          consumingEvent.getExercised.witnessParties.sorted == expectedWitnessesOfConsumingChoice,
          s"The parties for witnessing the consuming ExercisedEvent should be ${expectedWitnessesOfConsumingChoice}, but were ${consumingEvent.getCreated.witnessParties}",
        )

      }
  }
}
