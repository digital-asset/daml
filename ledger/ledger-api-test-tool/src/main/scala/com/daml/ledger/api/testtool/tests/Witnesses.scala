// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test.Test.DivulgeWitnesses._
import com.digitalasset.ledger.test.Test.Witnesses._
import com.digitalasset.ledger.test.Test.{DivulgeWitnesses, Witnesses => WitnessesTemplate}
import scalaz.Tag

final class Witnesses(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val respectDisclosureRules =
    LedgerTest("RespectDisclosureRules", "The ledger should respect disclosure rules") {
      implicit context =>
        for {
          Vector(alice, bob, charlie) <- allocateParties(3)

          // Create the Witnesses contract as Alice and get the resulting transaction as seen by all parties
          (witnessesTransactionId, witnesses) <- createAndGetTransactionId(
            WitnessesTemplate(alice, bob, charlie))(alice)
          witnessesTransaction <- transactionTreeById(witnessesTransactionId, alice, bob, charlie)

          // Charlie is not a stakeholder of Witnesses and thus cannot see any such contract unless divulged.
          // Such contract is divulged by creating a DivulgeWitness with Charlie as a stakeholder and exercising
          // a choice as Alice that causes divulgence (in this case, the Witnesses instance previously
          // created is fetched as part of the transaction).
          divulgeWitness <- create(DivulgeWitnesses(alice, charlie))(charlie)
          _ <- exercise(divulgeWitness.contractId.exerciseDivulge(_, witnesses.contractId))(alice)

          // A non-consuming choice is exercised with the expectation
          // that this will not cause the contract to be divulged
          // The tree is fetched from the identifier to ensure we get the witnesses as seen by all parties
          nonConsuming <- exercise(witnesses.contractId.exerciseWitnessesNonConsumingChoice)(
            charlie)
          nonConsumingTransaction <- transactionTreeById(
            nonConsuming.transactionId,
            alice,
            bob,
            charlie)

          // A consuming choice is exercised with the expectation
          // that this will cause the contract to be divulged
          // The tree is fetched from the identifier to ensure we get the witnesses as seen by all parties
          consumingTransaction <- exercise(witnesses.contractId.exerciseWitnessesChoice)(charlie)
          consumingTree <- transactionTreeById(
            consumingTransaction.transactionId,
            alice,
            bob,
            charlie)
        } yield {

          assert(
            witnessesTransaction.eventsById.size == 1,
            s"The transaction for creating the Witness contract should only contain a single event, but has ${witnessesTransaction.eventsById.size}"
          )
          val (_, witnessEv) = witnessesTransaction.eventsById.head
          assert(
            witnessEv.kind.isCreated,
            s"The event in the transaction for creating the Witness should be a CreatedEvent, but was ${witnessEv.kind}")

          val expectedWitnessesOfCreation = Tag.unsubst(Seq(alice, bob)).sorted
          assert(
            witnessEv.getCreated.witnessParties.sorted == expectedWitnessesOfCreation,
            s"The parties for witnessing the CreatedEvent should be ${expectedWitnessesOfCreation}, but were ${witnessEv.getCreated.witnessParties}"
          )
          assert(
            nonConsumingTransaction.eventsById.size == 1,
            s"The transaction for exercising the non-consuming choice should only contain a single event, but has ${nonConsumingTransaction.eventsById.size}"
          )
          val nonConsumingEv = nonConsumingTransaction.eventsById.head._2
          assert(
            nonConsumingEv.kind.isExercised,
            s"The event in the transaction for exercising the non-consuming choice should be an ExercisedEvent, but was ${nonConsumingEv.kind}"
          )

          val expectedWitnessesOfNonConsumingChoice = Tag.unsubst(Seq(alice, charlie)).sorted
          assert(
            nonConsumingEv.getExercised.witnessParties.sorted == expectedWitnessesOfNonConsumingChoice,
            s"The parties for witnessing the non-consuming ExercisedEvent should be ${expectedWitnessesOfNonConsumingChoice}, but were ${nonConsumingEv.getCreated.witnessParties}"
          )
          assert(
            consumingTree.eventsById.size == 1,
            s"The transaction for exercising the consuming choice should only contain a single event, but has ${consumingTree.eventsById.size}"
          )

          val (_, consumingEvent) = consumingTree.eventsById.head
          assert(
            consumingEvent.kind.isExercised,
            s"The event in the transaction for exercising the consuming choice should be an ExercisedEvent, but was ${consumingEvent.kind}"
          )
          val expectedWitnessesOfConsumingChoice = Tag.unsubst(Seq(alice, bob, charlie)).sorted
          assert(
            consumingEvent.getExercised.witnessParties.sorted == expectedWitnessesOfConsumingChoice,
            s"The parties for witnessing the consuming ExercisedEvent should be ${expectedWitnessesOfConsumingChoice}, but were ${consumingEvent.getCreated.witnessParties}"
          )

        }
    }

  override val tests: Vector[LedgerTest] = Vector(respectDisclosureRules)

}
