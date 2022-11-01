// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.test.model.Test._
import com.daml.ledger.errors.LedgerApiErrors
import scalaz.Tag

import scala.collection.immutable.Seq
import scala.concurrent.Future

class TransactionServiceStreamsIT extends LedgerTestSuite {
  test(
    "TXBeginToBegin",
    "An empty stream should be served when getting transactions from and to the beginning of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.getTransactionsRequest(ledger.transactionFilter(Seq(party)))
    val fromAndToBegin = request.update(_.begin := ledger.begin, _.end := ledger.begin)
    for {
      transactions <- ledger.flatTransactions(fromAndToBegin)
    } yield {
      assert(
        transactions.isEmpty,
        s"Received a non-empty stream with ${transactions.size} transactions in it.",
      )
    }
  })

  test(
    "TXTreesBeginToBegin",
    "An empty stream of trees should be served when getting transactions from and to the beginning of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.getTransactionsRequest(ledger.transactionFilter(Seq(party)))
    val fromAndToBegin = request.update(_.begin := ledger.begin, _.end := ledger.begin)
    for {
      transactions <- ledger.transactionTrees(fromAndToBegin)
    } yield {
      assert(
        transactions.isEmpty,
        s"Received a non-empty stream with ${transactions.size} transactions in it.",
      )
    }
  })

  test(
    "TXEndToEnd",
    "An empty stream should be served when getting transactions from and to the end of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      request = ledger.getTransactionsRequest(ledger.transactionFilter(Seq(party)))
      endToEnd = request.update(_.begin := ledger.end, _.end := ledger.end)
      transactions <- ledger.flatTransactions(endToEnd)
    } yield {
      assert(
        transactions.isEmpty,
        s"No transactions were expected but ${transactions.size} were read",
      )
    }
  })

  test(
    "TXAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing past the ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      futureOffset <- ledger.offsetBeyondLedgerEnd()
      request = ledger.getTransactionsRequest(ledger.transactionFilter(Seq(party)))
      beyondEnd = request.update(_.begin := futureOffset, _.optionalEnd := None)
      failure <- ledger.flatTransactions(beyondEnd).mustFail("subscribing past the ledger end")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.OffsetAfterLedgerEnd,
        Some("is after ledger end"),
      )
    }
  })

  test(
    "TXTreesAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing to trees past the ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      futureOffset <- ledger.offsetBeyondLedgerEnd()
      request = ledger.getTransactionsRequest(ledger.transactionFilter(Seq(party)))
      beyondEnd = request.update(_.begin := futureOffset, _.optionalEnd := None)
      failure <- ledger.transactionTrees(beyondEnd).mustFail("subscribing past the ledger end")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.OffsetAfterLedgerEnd,
        Some("is after ledger end"),
      )
    }
  })

  test(
    "TXServeUntilCancellation",
    "Items should be served until the client cancels",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party)))
      )
      transactions <- ledger.flatTransactions(transactionsToRead, party)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
    }
  })

  test(
    "TXServeTreesUntilCancellation",
    "Trees should be served until the client cancels",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 14
    val treesToRead = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party)))
      )
      trees <- ledger.transactionTrees(treesToRead, party)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        trees.size == treesToRead,
        s"$treesToRead should have been received but ${trees.size} were instead",
      )
    }
  })

  test(
    "TXCompleteOnLedgerEnd",
    "A stream should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.flatTransactions(party)
    for {
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  })

  test(
    "TXCompleteTreesOnLedgerEnd",
    "A stream of trees should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.transactionTrees(party)
    for {
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  })

  test(
    "TXFilterByTemplate",
    "The transaction service should correctly filter by template identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val filterBy = Dummy.id
    val create = ledger.submitAndWaitRequest(
      party,
      Dummy(party).create.command,
      DummyFactory(party).create.command,
    )
    for {
      _ <- ledger.submitAndWait(create)
      transactions <- ledger.flatTransactionsByTemplateId(filterBy, party)
    } yield {
      val contract = assertSingleton("FilterByTemplate", transactions.flatMap(createdEvents))
      assertEquals("FilterByTemplate", contract.getTemplateId, Tag.unwrap(filterBy))
    }
  })
}
