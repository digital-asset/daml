// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.value.RecordField
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test._

import scala.collection.immutable.Seq

class TransactionServiceOutputsIT extends LedgerTestSuite {
  test(
    "TXUnitAsArgumentToNothing",
    "Daml engine returns Unit as argument to Nothing",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = NothingArgument(party, Primitive.Optional.empty)
    val create = ledger.submitAndWaitRequest(party, template.create.command)
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val contract = assertSingleton(
        "UnitAsArgumentToNothing",
        createdEvents(transactionResponse.getTransaction),
      )
      assertEquals("UnitAsArgumentToNothing", contract.getCreateArguments, template.arguments)
    }
  })

  test(
    "TXAgreementTextExplicit",
    "Expose the agreement text for templates with an explicit agreement text",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      transactions <- ledger.flatTransactionsByTemplateId(Dummy.id, party)
    } yield {
      val contract = assertSingleton("AgreementText", transactions.flatMap(createdEvents))
      assertEquals("AgreementText", contract.getAgreementText, s"'$party' operates a dummy.")
    }
  })

  test(
    "TXAgreementTextDefault",
    "Expose the default text for templates without an agreement text",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, DummyWithParam(party))
      transactions <- ledger.flatTransactions(party)
    } yield {
      val contract = assertSingleton("AgreementTextDefault", transactions.flatMap(createdEvents))
      assertEquals("AgreementTextDefault", contract.getAgreementText, "")
    }
  })

  test(
    "TXVerbosity",
    "Expose field names only if the verbose flag is set to true",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      request = ledger.getTransactionsRequest(Seq(party))
      verboseTransactions <- ledger.flatTransactions(request.update(_.verbose := true))
      verboseTransactionTrees <- ledger.transactionTrees(request.update(_.verbose := true))
      nonVerboseTransactions <- ledger.flatTransactions(request.update(_.verbose := false))
      nonVerboseTransactionTrees <- ledger.transactionTrees(request.update(_.verbose := false))
    } yield {
      assertLabelsAreExposedCorrectly(
        party,
        verboseTransactions,
        verboseTransactionTrees,
        labelIsNonEmpty = true,
      )
      assertLabelsAreExposedCorrectly(
        party,
        nonVerboseTransactions,
        nonVerboseTransactionTrees,
        labelIsNonEmpty = false,
      )
    }
  })

  private def assertLabelsAreExposedCorrectly(
      party: Primitive.Party,
      transactions: Seq[Transaction],
      transactionTrees: Seq[TransactionTree],
      labelIsNonEmpty: Boolean,
  ): Unit = {

    def transactionFields(createdEvent: Seq[CreatedEvent]): Seq[RecordField] = createdEvent
      .flatMap(_.getCreateArguments.fields)

    val transactionTreeCreatedEvents: Seq[CreatedEvent] = {
      for {
        transactionTree <- transactionTrees
        eventById <- transactionTree.eventsById
        (_, tree) = eventById
        createdEvent = tree.getCreated
      } yield createdEvent
    }

    val transactionTreeFields: Seq[RecordField] =
      transactionFields(transactionTreeCreatedEvents)

    val flatTransactionFields: Seq[RecordField] =
      transactionFields(
        transactions
          .flatMap(_.events)
          .map(_.getCreated)
      )

    assert(transactions.nonEmpty, s"$party expected non empty transaction list")
    assert(transactionTrees.nonEmpty, s"$party expected non empty transaction tree list")

    val text = labelIsNonEmpty match {
      case true => "with"
      case false => "without"
    }
    assert(
      flatTransactionFields.forall(_.label.nonEmpty == labelIsNonEmpty),
      s"$party expected a contract $text labels, but received $transactions.",
    )
    assert(
      transactionTreeFields.forall(_.label.nonEmpty == labelIsNonEmpty),
      s"$party expected a contract $text labels, but received $transactionTrees.",
    )
  }

}
