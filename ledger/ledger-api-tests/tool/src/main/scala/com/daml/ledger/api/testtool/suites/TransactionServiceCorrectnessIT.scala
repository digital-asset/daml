// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.suites.TransactionServiceCorrectnessIT._
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.ledger.test.model.Test._
import com.daml.platform.api.v1.event.EventOps.{EventOps, TreeEventOps}

import scala.collection.immutable.Seq
import scala.concurrent.Future

class TransactionServiceCorrectnessIT extends LedgerTestSuite {
  test(
    "TXProcessInTwoChunks",
    "Serve the complete sequence of transactions even if processing is stopped and resumed",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 5
    for {
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      endAfterFirstSection <- ledger.currentEnd()
      firstSectionRequest = ledger
        .getTransactionsRequest(Seq(party))
        .update(_.end := endAfterFirstSection)
      firstSection <- ledger.flatTransactions(firstSectionRequest)
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      endAfterSecondSection <- ledger.currentEnd()
      secondSectionRequest = ledger
        .getTransactionsRequest(Seq(party))
        .update(_.begin := endAfterFirstSection, _.end := endAfterSecondSection)
      secondSection <- ledger.flatTransactions(secondSectionRequest)
      fullSequence <- ledger.flatTransactions(party)
    } yield {
      val concatenation = Vector.concat(firstSection, secondSection)
      assert(
        fullSequence == concatenation,
        s"The result of processing items in two chunk should yield the same result as getting the overall stream of transactions in the end but there are differences. " +
          s"Full sequence: ${fullSequence.map(_.commandId).mkString(", ")}, " +
          s"first section: ${firstSection.map(_.commandId).mkString(", ")}, " +
          s"second section: ${secondSection.map(_.commandId).mkString(", ")}",
      )
    }
  })

  test(
    "TXParallel",
    "The same data should be served for more than 1 identical, parallel requests",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 5
    val parallelRequests = 10
    for {
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      results <- Future.sequence(Vector.fill(parallelRequests)(ledger.flatTransactions(party)))
    } yield {
      assert(
        results.toSet.size == 1,
        s"All requests are supposed to return the same results but there " +
          s"where differences: ${results.map(_.map(_.commandId)).mkString(", ")}",
      )
    }
  })

  test(
    "TXSingleMultiSameBasic",
    "The same transaction should be served regardless of subscribing as one or multiple parties",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- ledger.create(alice, Dummy(alice))
      _ <- ledger.create(bob, Dummy(bob))
      aliceView <- ledger.flatTransactions(alice)
      bobView <- ledger.flatTransactions(bob)
      multiSubscriptionView <- ledger.flatTransactions(alice, bob)
    } yield {
      val jointView = aliceView ++ bobView
      assertEquals(
        "Single- and multi-party subscription yield different results",
        jointView,
        multiSubscriptionView,
      )
    }
  })

  test(
    "TXSingleMultiSameTreesBasic",
    "The same transaction trees should be served regardless of subscribing as one or multiple parties",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- ledger.create(alice, Dummy(alice))
      _ <- ledger.create(bob, Dummy(bob))
      aliceView <- ledger.transactionTrees(alice)
      bobView <- ledger.transactionTrees(bob)
      multiSubscriptionView <- ledger.transactionTrees(alice, bob)
    } yield {
      val jointView = aliceView ++ bobView
      assertEquals(
        "Single- and multi-party subscription yield different results",
        jointView,
        multiSubscriptionView,
      )
    }
  })

  test(
    "TXSingleMultiSameStakeholders",
    "The same transaction should be served to all stakeholders",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      _ <- alpha.create(alice, AgreementFactory(bob, alice))
      _ <- beta.create(bob, AgreementFactory(alice, bob))
      _ <- synchronize(alpha, beta)
      alphaView <- alpha.flatTransactions(alice, bob)
      betaView <- beta.flatTransactions(alice, bob)
    } yield {
      assertEquals(
        "Single- and multi-party subscription yield different results",
        comparableTransactions(alphaView),
        comparableTransactions(betaView),
      )
    }
  })

  test(
    "TXSingleMultiSameTreesStakeholders",
    "The same transaction trees should be served to all stakeholders",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      _ <- alpha.create(alice, AgreementFactory(bob, alice))
      _ <- beta.create(bob, AgreementFactory(alice, bob))
      _ <- synchronize(alpha, beta)
      alphaView <- alpha.transactionTrees(alice, bob)
      betaView <- beta.transactionTrees(alice, bob)
    } yield {
      assertEquals(
        "Single- and multi-party subscription yield different results",
        comparableTransactionTrees(alphaView),
        comparableTransactionTrees(betaView),
      )
    }
  })

  test(
    "TXTransactionTreeByIdSameAsTransactionStream",
    "Expose the same events for each transaction as the output of getTransactionTrees",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, submitter), Participant(beta, listener)) =>
      for {
        _ <- alpha.create(submitter, AgreementFactory(listener, submitter))
        _ <- synchronize(alpha, beta)
        trees <- alpha.transactionTrees(listener, submitter)
        byId <- Future.sequence(
          trees.map(t => beta.transactionTreeById(t.transactionId, listener, submitter))
        )
      } yield {
        assertEquals(
          "The events fetched by identifier did not match the ones on the transaction stream",
          comparableTransactionTrees(trees),
          comparableTransactionTrees(byId),
        )
      }
  })

  test(
    "TXFlatTransactionByIdSameAsTransactionStream",
    "Expose the same events for each transaction as the output of getTransactions",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, submitter), Participant(beta, listener)) =>
      for {
        _ <- alpha.create(submitter, AgreementFactory(listener, submitter))
        _ <- synchronize(alpha, beta)
        transactions <- alpha.flatTransactions(listener, submitter)
        byId <- Future.sequence(
          transactions.map(t => beta.flatTransactionById(t.transactionId, listener, submitter))
        )
      } yield {
        assertEquals(
          "The events fetched by identifier did not match the ones on the transaction stream",
          comparableTransactions(transactions),
          comparableTransactions(byId),
        )
      }
  })

  test(
    "TXFlatSubsetOfTrees",
    "The event identifiers in the flat stream should be a subset of those in the trees stream",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, Dummy(party))
            .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))
        )
      )
      transactions <- ledger.flatTransactions(party)
      trees <- ledger.transactionTrees(party)
    } yield {
      assert(
        transactions
          .flatMap(
            _.events.map(e =>
              e.event.archived.map(_.eventId).orElse(e.event.created.map(_.eventId)).get
            )
          )
          .toSet
          .subsetOf(trees.flatMap(_.eventsById.keys).toSet)
      )
    }
  })

  test(
    "TXFlatWitnessesSubsetOfTrees",
    "The witnesses in the flat stream should be a subset of those in the trees stream",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, Dummy(party))
            .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))
        )
      )
      transactions <- ledger.flatTransactions(party)
      trees <- ledger.transactionTrees(party)
    } yield {
      val witnessesByEventIdInTreesStream =
        trees.iterator
          .flatMap(_.eventsById)
          .map { case (id, event) =>
            id -> event.kind.exercised
              .map(_.witnessParties.toSet)
              .orElse(event.kind.created.map(_.witnessParties.toSet))
              .get
          }
          .toMap
      val witnessesByEventIdInFlatStream =
        transactions
          .flatMap(
            _.events.map(e =>
              e.event.archived
                .map(a => a.eventId -> a.witnessParties.toSet)
                .orElse(e.event.created.map(c => c.eventId -> c.witnessParties.toSet))
                .get
            )
          )
      for ((event, witnesses) <- witnessesByEventIdInFlatStream) {
        assert(witnesses.subsetOf(witnessesByEventIdInTreesStream(event)))
      }
    }
  })

  test(
    "TXSingleSubscriptionInOrder",
    "Archives should always come after creations when subscribing as a single party",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, Dummy(party))
            .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))
        )
      )
      transactions <- ledger.flatTransactions(party)
    } yield {
      checkTransactionsOrder("Ledger", transactions, contracts)
    }
  })

  test(
    "TXMultiSubscriptionInOrder",
    "Archives should always come after creations when subscribing as more than on party",
    allocate(TwoParties),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val contracts = 50
    for {
      _ <- Future.sequence(Vector.tabulate(contracts) { n =>
        val party = if (n % 2 == 0) alice else bob
        ledger
          .create(party, Dummy(party))
          .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))
      })
      transactions <- ledger.flatTransactions(alice, bob)
    } yield {
      checkTransactionsOrder("Ledger", transactions, contracts)
    }
  })
}

object TransactionServiceCorrectnessIT {
  // Strip command id and offset to yield a transaction comparable across participant
  // Furthermore, makes sure that the order is not relevant for witness parties
  // Sort by transactionId as on distributed ledgers updates can occur in different orders
  private def comparableTransactions(transactions: Vector[Transaction]): Vector[Transaction] =
    transactions
      .map(t =>
        t.copy(
          commandId = "commandId",
          offset = "offset",
          events = t.events.map(_.modifyWitnessParties(_.sorted)),
        )
      )
      .sortBy(_.transactionId)

  // Strip command id and offset to yield a transaction comparable across participant
  // Furthermore, makes sure that the order is not relevant for witness parties
  // Sort by transactionId as on distributed ledgers updates can occur in different orders
  private def comparableTransactionTrees(
      transactionTrees: Vector[TransactionTree]
  ): Vector[TransactionTree] =
    transactionTrees
      .map(t =>
        t.copy(
          commandId = "commandId",
          offset = "offset",
          eventsById = t.eventsById.view.mapValues(_.modifyWitnessParties(_.sorted)).toMap,
        )
      )
      .sortBy(_.transactionId)

  private def checkTransactionsOrder(
      context: String,
      transactions: Vector[Transaction],
      contracts: Int,
  ): Unit = {
    val (cs, as) =
      transactions.flatMap(_.events).zipWithIndex.partition { case (e, _) =>
        e.event.isCreated
      }
    val creations = cs.map { case (e, i) => e.getCreated.contractId -> i }
    val archivals = as.map { case (e, i) => e.getArchived.contractId -> i }
    assert(
      creations.size == contracts && archivals.size == contracts,
      s"$context: either the number of archive events (${archivals.size}) or the number of create events (${creations.size}) doesn't match the expected number of $contracts.",
    )
    val createdContracts = creations.iterator.map(_._1).toSet
    val archivedContracts = archivals.iterator.map(_._1).toSet
    assert(
      createdContracts.size == creations.size,
      s"$context: there are duplicate contract identifiers in the create events",
    )
    assert(
      archivedContracts.size == archivals.size,
      s"$context: there are duplicate contract identifiers in the archive events",
    )
    assert(
      createdContracts == archivedContracts,
      s"$context: the contract identifiers for created and archived contracts differ: ${createdContracts
        .diff(archivedContracts)}",
    )
    val sortedCreations = creations.sortBy(_._1)
    val sortedArchivals = archivals.sortBy(_._1)
    for (i <- 0 until contracts) {
      val (createdContract, creationIndex) = sortedCreations(i)
      val (archivedContract, archivalIndex) = sortedArchivals(i)
      assert(
        createdContract == archivedContract,
        s"$context: unexpected discrepancy between the created and archived events",
      )
      assert(
        creationIndex < archivalIndex,
        s"$context: the creation of $createdContract did not appear in the stream before it's archival",
      )
    }
  }
}
