// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test.Test.DivulgeWitnesses._
import com.digitalasset.ledger.test.Test.Witnesses._
import com.digitalasset.ledger.test.Test.{DivulgeWitnesses, Witnesses => WitnessesTemplate}
import scalaz.Tag

final class Witnesses(session: LedgerSession) extends LedgerTestSuite(session) {

  val respectDisclosureRules =
    LedgerTest("RespectDisclosureRules", "The ledger should respect disclosure rules") {
      implicit context =>
        for {
          Vector(alice, bob, charlie) <- allocateParties(3)

          // Create Witnesses contract
          (witnessTxId, witnesses) <- create(WitnessesTemplate(alice, bob, charlie))(alice)
          witnessTx <- transactionTreeById(witnessTxId, alice, bob, charlie)

          // Divulge Witnesses contract to charlie, who's just an actor and thus cannot
          // see it by default.
          (_, divulgeWitness) <- create(DivulgeWitnesses(alice, charlie))(charlie)
          _ <- exercise(divulgeWitness.contractId.exerciseDivulge(alice, witnesses.contractId))(
            alice)

          // Now, first try the non-consuming choice
          nonConsumingTxId <- exercise(
            witnesses.contractId.exerciseWitnessesNonConsumingChoice(charlie))(charlie)
          nonConsumingTx <- transactionTreeById(nonConsumingTxId, alice, bob, charlie)

          // And then the consuming one
          consumingTxId <- exercise(witnesses.contractId.exerciseWitnessesChoice(charlie))(charlie)
          consumingTx <- transactionTreeById(consumingTxId, alice, bob, charlie)
        } yield {
          {
            assert(
              witnessTx.eventsById.size == 1,
              s"The transaction for creating the Witness contract should only contain a single event, but has ${witnessTx.eventsById.size}"
            )
            val witnessEv = witnessTx.eventsById.head._2
            assert(
              witnessEv.kind.isCreated,
              s"The event in the transaction for creating the Witness should be a CreatedEvent, but was ${witnessEv.kind}")

            // stakeholders = signatories \cup observers
            val expectedWitnesses = Tag.unsubst(Seq(alice, bob)).sorted
            assert(
              witnessEv.getCreated.witnessParties.sorted == expectedWitnesses,
              s"The parties for witnessing the CreatedEvent should be ${expectedWitnesses}, but were ${witnessEv.getCreated.witnessParties}"
            )
          }

          {
            assert(
              nonConsumingTx.eventsById.size == 1,
              s"The transaction for exercising the non-consuming choice should only contain a single event, but has ${nonConsumingTx.eventsById.size}"
            )
            val nonConsumingEv = nonConsumingTx.eventsById.head._2
            assert(
              nonConsumingEv.kind.isExercised,
              s"The event in the transaction for exercising the non-consuming choice should be an ExercisedEvent, but was ${nonConsumingEv.kind}"
            )
            // signatories \cup actors
            val expectedWitnesses = Tag.unsubst(Seq(alice, charlie)).sorted
            assert(
              nonConsumingEv.getExercised.witnessParties.sorted == expectedWitnesses,
              s"The parties for witnessing the non-consuming ExercisedEvent should be ${expectedWitnesses}, but were ${nonConsumingEv.getCreated.witnessParties}"
            )
          }

          {
            assert(
              consumingTx.eventsById.size == 1,
              s"The transaction for exercising the consuming choice should only contain a single event, but has ${consumingTx.eventsById.size}"
            )
            val consumingEv = consumingTx.eventsById.head._2
            assert(
              consumingEv.kind.isExercised,
              s"The event in the transaction for exercising the consuming choice should be an ExercisedEvent, but was ${consumingEv.kind}"
            )
            // stakeholders \cup actors
            val expectedWitnesses = Tag.unsubst(Seq(alice, bob, charlie)).sorted
            assert(
              consumingEv.getExercised.witnessParties.sorted == expectedWitnesses,
              s"The parties for witnessing the consuming ExercisedEvent should be ${expectedWitnesses}, but were ${consumingEv.getCreated.witnessParties}"
            )
          }

        }
    }
  override val tests: Vector[LedgerTest] = Vector(respectDisclosureRules)
}
