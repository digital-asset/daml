// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
}
import com.daml.ledger.test.java.model.test.Witnesses
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects

final class WitnessesIT extends LedgerTestSuite {
  test(
    "RespectDisclosureRules",
    "The ledger should respect disclosure rules",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie))) =>
    for {
      // Create the Witnesses contract as Alice
      (_, witnesses) <- ledger
        .createAndGetTransactionId(
          alice,
          new Witnesses(alice, bob, charlie),
        )(Witnesses.COMPANION)
      txReq <- ledger.getTransactionsRequest(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = Seq(alice, bob, charlie)
                .map(party =>
                  party.getValue -> new Filters(
                    Seq(
                      CumulativeFilter(
                        IdentifierFilter.TemplateFilter(
                          TemplateFilter(
                            Some(Witnesses.TEMPLATE_ID.toV1),
                            includeCreatedEventBlob = true,
                          )
                        )
                      )
                    )
                  )
                )
                .toMap,
              filtersForAnyParty = None,
              verbose = false,
            )
          ),
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )
      )
      witnessesTransactions <- ledger.transactions(txReq)
      witnessesTransaction = witnessesTransactions.head
      witnessesFromTransaction = witnessesTransaction.events.head.getCreated
      disclosedWitnesses = DisclosedContract(
        templateId = witnessesFromTransaction.templateId,
        contractId = witnessesFromTransaction.contractId,
        createdEventBlob = witnessesFromTransaction.createdEventBlob,
        synchronizerId = "",
      )

      // A non-consuming choice is exercised with the expectation
      // that Charlie is able to exercise a choice on an explicitly disclosed contract
      // The ledger effects transaction is fetched from the identifier to ensure we get the witnesses as seen by all parties
      nonConsuming <- ledger.submitAndWaitForTransaction(
        ledger
          .submitAndWaitForTransactionRequest(
            party = charlie,
            commands = witnesses.exerciseWitnessesNonConsumingChoice().commands,
            transactionShape = LedgerEffects,
            filterParties = Some(Seq(charlie)),
          )
          .update(_.commands.disclosedContracts := Seq(disclosedWitnesses))
      )
      nonConsumingLedgerEffects <- ledger.transactionById(
        nonConsuming.getTransaction.updateId,
        Seq(alice, bob, charlie),
        LedgerEffects,
      )

      // A consuming choice is exercised with the expectation
      // that Charlie is able to exercise a choice on an explicitly disclosed contract
      // The ledger effects transaction is fetched from the identifier to ensure we get the witnesses as seen by all parties
      consuming <- ledger.submitAndWaitForTransaction(
        ledger
          .submitAndWaitForTransactionRequest(
            charlie,
            witnesses.exerciseWitnessesChoice().commands,
            LedgerEffects,
          )
          .update(_.commands.disclosedContracts := Seq(disclosedWitnesses))
      )
      consumingLedgerEffects <- ledger.transactionById(
        consuming.getTransaction.updateId,
        Seq(
          alice,
          bob,
          charlie,
        ),
        LedgerEffects,
      )
    } yield {
      assert(
        witnessesTransactions.size == 1,
        s"There should be one transaction for Witnesses, but there was ${witnessesTransactions.size}",
      )

      assert(
        witnessesTransaction.events.sizeIs == 1,
        s"The transaction for creating the Witness contract should only contain a single event, but has ${witnessesTransaction.events.size}",
      )
      val creationEvent = witnessesTransaction.events.head
      assert(
        creationEvent.event.isCreated,
        s"The event in the transaction for creating the Witness should be a CreatedEvent, but was ${creationEvent.event}",
      )

      val expectedWitnessesOfCreation = Seq(alice, bob).map(_.getValue).sorted
      assert(
        creationEvent.getCreated.witnessParties.sorted == expectedWitnessesOfCreation,
        s"The parties for witnessing the CreatedEvent should be $expectedWitnessesOfCreation, but were ${creationEvent.getCreated.witnessParties}",
      )
      assert(
        nonConsumingLedgerEffects.events.size == 1,
        s"The transaction for exercising the non-consuming choice should only contain a single event, but has ${nonConsumingLedgerEffects.events.size}",
      )
      val nonConsumingEvent = nonConsumingLedgerEffects.events.head
      assert(
        nonConsumingEvent.event.isExercised,
        s"The event in the transaction for exercising the non-consuming choice should be an ExercisedEvent, but was ${nonConsumingEvent.event}",
      )

      val expectedWitnessesOfNonConsumingChoice = Seq(alice, charlie).map(_.getValue).sorted
      assert(
        nonConsumingEvent.getExercised.witnessParties.sorted == expectedWitnessesOfNonConsumingChoice,
        s"The parties for witnessing the non-consuming ExercisedEvent should be $expectedWitnessesOfNonConsumingChoice, but were ${nonConsumingEvent.getCreated.witnessParties}",
      )
      assert(
        consumingLedgerEffects.events.size == 1,
        s"The transaction for exercising the consuming choice should only contain a single event, but has ${consumingLedgerEffects.events.size}",
      )

      val consumingEvent = consumingLedgerEffects.events.head
      assert(
        consumingEvent.event.isExercised,
        s"The event in the transaction for exercising the consuming choice should be an ExercisedEvent, but was ${consumingEvent.event}",
      )
      val expectedWitnessesOfConsumingChoice = Seq(alice, bob, charlie).map(_.getValue).sorted
      assert(
        consumingEvent.getExercised.witnessParties.sorted == expectedWitnessesOfConsumingChoice,
        s"The parties for witnessing the consuming ExercisedEvent should be $expectedWitnessesOfConsumingChoice, but were ${consumingEvent.getCreated.witnessParties}",
      )

    }
  })
}
