// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.suites.v2_1.TransactionServiceCorrectnessIT.*
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.event.Event.Event.{Archived, Created, Empty, Exercised}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.test.java.model.test.{AgreementFactory, Dummy}
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.platform.store.utils.EventOps.EventOps

import java.time.Instant
import scala.collection.immutable.Seq
import scala.concurrent.Future

class TransactionServiceCorrectnessIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "TXProcessInTwoChunks",
    "Serve the complete sequence of transactions even if processing is stopped and resumed",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 5
    for {
      _ <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      endAfterFirstSection <- ledger.currentEnd()
      firstSectionRequest <- ledger
        .getTransactionsRequest(ledger.transactionFormat(Some(Seq(party)), verbose = true))
        .map(
          _.update(
            _.endInclusive := endAfterFirstSection
          )
        )
      firstSection <- ledger.transactions(firstSectionRequest)
      _ <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      secondSectionRequest <- ledger
        .getTransactionsRequest(ledger.transactionFormat(Some(Seq(party)), verbose = true))
        .map(
          _.update(
            _.beginExclusive := endAfterFirstSection
          )
        )
      secondSection <- ledger.transactions(secondSectionRequest)
      fullSequence <- ledger.transactions(AcsDelta, party)
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
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val transactionsToSubmit = 5
    val parallelRequests = 10
    for {
      _ <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, new Dummy(party)))
      )
      results <- Future.sequence(
        Vector.fill(parallelRequests)(ledger.transactions(AcsDelta, party))
      )
    } yield {
      assert(
        results.toSet.sizeIs == 1,
        s"All requests are supposed to return the same results but there " +
          s"where differences: ${results.map(_.map(_.commandId)).mkString(", ")}",
      )
    }
  })

  test(
    "TXSingleMultiSameBasic",
    "The same transaction should be served regardless of subscribing as one or multiple parties",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      _ <- ledger.create(alice, new Dummy(alice))
      _ <- ledger.create(bob, new Dummy(bob))
      aliceView <- ledger.transactions(AcsDelta, alice)
      bobView <- ledger.transactions(AcsDelta, bob)
      multiSubscriptionView <- ledger.transactions(AcsDelta, alice, bob)
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
    "TXSingleMultiSameLedgerEffectsBasic",
    "The same ledger effects transactions should be served regardless of subscribing as one, multiple or wildcard parties",
    allocate(TwoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      _ <- ledger.create(alice, new Dummy(alice))
      _ <- ledger.create(bob, new Dummy(bob))
      aliceView <- ledger.transactions(LedgerEffects, alice)
      bobView <- ledger.transactions(LedgerEffects, bob)
      multiSubscriptionView <- ledger.transactions(LedgerEffects, alice, bob)
      txReq <- ledger.getTransactionsRequest(
        transactionFormat =
          ledger.transactionFormat(None, transactionShape = LedgerEffects, verbose = true),
        begin = endOffsetAtTestStart,
      )
      wildcardPartyView <- ledger.transactions(txReq)
    } yield {
      val jointView = aliceView ++ bobView
      assertEquals(
        "Single- and multi-party subscription yield different results",
        jointView,
        multiSubscriptionView,
      )
      assertEquals(
        "Multi-party and wildcard subscription yield different results",
        wildcardPartyView,
        multiSubscriptionView,
      )
    }
  })

  test(
    "TXSingleMultiSameStakeholders",
    "The same transaction should be served to all stakeholders",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(alice)), Participant(beta, Seq(bob))) =>
      for {
        _ <- alpha.create(alice, new AgreementFactory(bob, alice))
        _ <- beta.create(bob, new AgreementFactory(alice, bob))
        _ <- p.synchronize
        alphaView <- alpha.transactions(AcsDelta, alice, bob)
        betaView <- beta.transactions(AcsDelta, alice, bob)
      } yield {
        verifyLength("Expected to get 2 transactions", 2, alphaView)
        assertEquals(
          "Single- and multi-party subscription yield different results",
          comparableTransactions(alphaView),
          comparableTransactions(betaView),
        )
      }
  })

  test(
    "TXSingleMultiSameLedgerEffectsStakeholders",
    "The same ledger effects transactions should be served to all stakeholders",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(alice)), Participant(beta, Seq(bob))) =>
      for {
        _ <- alpha.create(alice, new AgreementFactory(bob, alice))
        _ <- beta.create(bob, new AgreementFactory(alice, bob))
        _ <- p.synchronize
        alphaView <- alpha.transactions(LedgerEffects, alice, bob)
        betaView <- beta.transactions(LedgerEffects, alice, bob)
      } yield {
        verifyLength("Expected to get 2 transactions", 2, alphaView)
        assertEquals(
          "Single- and multi-party subscription yield different results",
          comparableTransactions(alphaView),
          comparableTransactions(betaView),
        )
      }
  })

  test(
    "TXTransactionByIdLedgerEffectsSameAsTransactionStream",
    "Expose the same events for each transaction as the output of getTransactions with Ledger Effects",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(submitter)), Participant(beta, Seq(listener))) =>
      for {
        _ <- alpha.create(submitter, new AgreementFactory(listener, submitter))
        _ <- p.synchronize
        transactions <- alpha.transactions(LedgerEffects, listener, submitter)
        byId <- Future.sequence(
          transactions.map(t =>
            beta.transactionById(t.updateId, Seq(listener, submitter), LedgerEffects)
          )
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
    "TXTransactionByIdAcsDeltaSameAsTransactionStream",
    "Expose the same events for each transaction as the output of getTransactions",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(submitter)), Participant(beta, Seq(listener))) =>
      for {
        _ <- alpha.create(submitter, new AgreementFactory(listener, submitter))
        _ <- p.synchronize
        transactions <- alpha.transactions(AcsDelta, listener, submitter)
        byId <- Future.sequence(
          transactions.map(t =>
            beta.transactionById(t.updateId, Seq(listener, submitter), AcsDelta)
          )
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
    "TXAcsDeltaTransactionEvents",
    "Expose offset and nodeId for events of each transaction",
    allocate(
      SingleParty
    ),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    for {
      contract <- alpha.create(party, new Dummy(party))
      _ <- alpha.exercise(party, contract.exerciseDummyChoice1())
      transactions <- alpha.transactions(AcsDelta, party)
    } yield {
      val txsWithCreated = transactions.filter(_.events.exists(_.event.isCreated))
      val txWithCreated = assertSingleton(
        "Only one transaction should contain the create event",
        txsWithCreated,
      )
      val createdEvents = txWithCreated.events.flatMap(_.event.created)
      val createdEvent = assertSingleton(
        "Transaction should contain a single created event",
        createdEvents,
      )
      assertEquals(
        "The created event offset should match the transaction offset",
        txWithCreated.offset,
        createdEvent.offset,
      )

      val txsWithArchived = transactions.filter(_.events.exists(_.event.isArchived))
      val txWithArchived = assertSingleton(
        "Only one transaction should contain the archived event",
        txsWithArchived,
      )
      val archivedEvents = txWithArchived.events.flatMap(_.event.archived)
      val archivedEvent = assertSingleton(
        "Transaction should contain the archived event",
        archivedEvents,
      )
      assertEquals(
        "The archived event offset should match the transaction offset",
        txWithArchived.offset,
        archivedEvent.offset,
      )
    }
  })

  test(
    "TXTransactionLedgerEffectsEvents",
    "Expose offset and nodeId for events of each transaction",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    for {
      contract <- alpha.create(party, new Dummy(party))
      _ <- alpha.exercise(party, contract.exerciseDummyChoice1())
      txsLedgerEffects <- alpha.transactions(LedgerEffects, party)
    } yield {
      val txsWithCreated = txsLedgerEffects.filter(_.events.view.exists(_.event.isCreated))
      val txWithCreated = assertSingleton(
        "Only one transaction should contain the create event",
        txsWithCreated,
      )
      val createdEvents = txWithCreated.events.view.map(_.getCreated)
      val createdEvent = assertSingleton(
        "Transaction should contain the create event",
        createdEvents.toSeq,
      )
      assertEquals(
        "The created event offset should match the transaction offset",
        txWithCreated.offset,
        createdEvent.offset,
      )

      val txsWithExercised = txsLedgerEffects.filter(_.events.view.exists(_.event.isExercised))

      val txWithExercised = assertSingleton(
        "Only one transaction should contain the exercise event",
        txsWithExercised,
      )
      val exercisedEvents = txWithExercised.events.view.map(_.getExercised)

      val exercisedEvent = assertSingleton(
        "Transaction should contain the exercise event",
        exercisedEvents.toSeq,
      )
      assertEquals(
        "The exercise event offset should match the transaction offset",
        txWithExercised.offset,
        exercisedEvent.offset,
      )
    }
  })

  test(
    "TXAcsDeltaSubsetOfLedgerEffects",
    "The event identifiers in the acs delta stream should be a subset of those in the ledger effects stream",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, new Dummy(party))
            .flatMap((contract: Dummy.ContractId) =>
              ledger.exercise(party, contract.exerciseDummyChoice1())
            )
        )
      )
      transactionsAcsDelta <- ledger.transactions(AcsDelta, party)
      transactionsLedgerEffects <- ledger.transactions(LedgerEffects, party)
    } yield {
      assert(
        transactionsAcsDelta
          .flatMap(_.events.map(_.nodeId))
          .toSet
          .subsetOf(
            transactionsLedgerEffects
              .flatMap(_.events.map(_.nodeId))
              .toSet
          )
      )
    }
  })

  test(
    "TXAcsDeltaWitnessesSubsetOfLedgerEffects",
    "The witnesses in the acs delta stream should be a subset of those in the ledger effects stream",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, new Dummy(party))
            .flatMap((contract: Dummy.ContractId) =>
              ledger.exercise(party, contract.exerciseDummyChoice1())
            )
        )
      )
      transactionsAcsDelta <- ledger.transactions(AcsDelta, party)
      transactionsLedgerEffects <- ledger.transactions(LedgerEffects, party)
    } yield {
      val witnessesByEventIdInLedgerEffectsStream =
        transactionsLedgerEffects
          .flatMap(_.events)
          .map(event => event.nodeId -> event.witnessParties.toSet)
          .toMap
      val witnessesByEventIdInAcsDeltaStream =
        transactionsAcsDelta
          .flatMap(_.events)
          .map(event => event.nodeId -> event.witnessParties.toSet)
      for ((event, witnesses) <- witnessesByEventIdInAcsDeltaStream) {
        assert(witnesses.subsetOf(witnessesByEventIdInLedgerEffectsStream(event)))
      }
    }
  })

  test(
    "TXSingleSubscriptionInOrder",
    "Archives should always come after creations when subscribing as a single party",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, new Dummy(party))
            .flatMap((contract: Dummy.ContractId) =>
              ledger.exercise(party, contract.exerciseDummyChoice1())
            )
        )
      )
      transactions <- ledger.transactions(AcsDelta, party)
    } yield {
      checkTransactionsOrder("Ledger", transactions, contracts)
    }
  })

  test(
    "TXMultiSubscriptionInOrder",
    "Archives should always come after creations when subscribing as more than on party",
    allocate(TwoParties),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    val contracts = 50
    for {
      _ <- Future.sequence(Vector.tabulate(contracts) { n =>
        val party = if (n % 2 == 0) alice else bob
        ledger
          .create(party, new Dummy(party))
          .flatMap((contract: Dummy.ContractId) =>
            ledger.exercise(party, contract.exerciseDummyChoice1())
          )
      })
      transactions <- ledger.transactions(AcsDelta, alice, bob)
    } yield {
      checkTransactionsOrder("Ledger", transactions, contracts)
    }
  })
}

object TransactionServiceCorrectnessIT {

  // Erase span id from the trace context. It is an element of the trace context that
  // is different on the different participants that are handling the transaction stream
  // requests. See https://www.w3.org/TR/trace-context/#header-name for the format details.
  def eraseSpanId(parentTraceId: String): String =
    parentTraceId.split("-").toList match {
      case ver :: traceId :: _ :: rest =>
        (ver :: traceId :: "0123456789abcdef" :: rest).mkString("-")
      case _ => parentTraceId
    }

  // Strip command id, offset, event id, node id and transaction id to yield a transaction comparable across participants
  // Furthermore, makes sure that the order is not relevant for witness parties
  // Sort by updateId as on distributed ledgers updates can occur in different orders
  // Even if updateIds are not the same across distributes ledgers, we still can use them for sorting
  private def comparableTransactions(transactions: Vector[Transaction]): Vector[Transaction] = {
    def stripEventFields(event: Event) =
      event match {
        case Archived(value) => Archived(value.copy(offset = 0L, nodeId = 0))
        case Created(value) => Created(value.copy(offset = 0L, nodeId = 0))
        case Exercised(value) => Exercised(value.copy(offset = 0L, nodeId = 0))
        case Empty => Empty
      }

    transactions
      .sortBy(_.updateId)
      .map(t =>
        t.copy(
          commandId = "commandId",
          offset = 12345678L,
          events = t.events
            .map(e => e.copy(event = stripEventFields(e.event)).modifyWitnessParties(_.sorted)),
          updateId = "updateId",
          traceContext = t.traceContext.map(tc => tc.copy(tc.traceparent.map(eraseSpanId))),
        )
      )
  }

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
      creations.sizeIs == contracts && archivals.sizeIs == contracts,
      s"$context: either the number of archive events (${archivals.size}) or the number of create events (${creations.size}) doesn't match the expected number of $contracts.",
    )
    val createdContracts = creations.iterator.map(_._1).toSet
    val archivedContracts = archivals.iterator.map(_._1).toSet
    assert(
      createdContracts.sizeIs == creations.size,
      s"$context: there are duplicate contract identifiers in the create events",
    )
    assert(
      archivedContracts.sizeIs == archivals.size,
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

    transactions.map(_.recordTime.value.asJavaInstant).foldLeft(Instant.MIN) {
      case (previous, current) if previous isBefore current => current
      case _ => fail(s"$context: record time of subsequent transactions was not increasing")
    }: Unit

  }

}
