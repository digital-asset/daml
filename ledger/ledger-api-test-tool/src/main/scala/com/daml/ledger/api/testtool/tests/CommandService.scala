// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test_stable.Test.Dummy
import io.grpc.Status
import scalaz.syntax.tag._

final class CommandService(session: LedgerSession) extends LedgerTestSuite(session) {
  private val submitAndWaitTest =
    LedgerTest("CSsubmitAndWait", "SubmitAndWait creates a contract of the expected template") {
      context =>
        for {
          ledger <- context.participant()
          alice <- ledger.allocateParty()
          request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
          _ <- ledger.submitAndWait(request)
          active <- ledger.activeContracts(alice)
        } yield {
          assert(active.size == 1)
          val dummyTemplateId = active.flatMap(_.templateId.toList).head
          assert(dummyTemplateId == Dummy.id.unwrap)
        }
    }

  private val submitAndWaitForTransactionIdTest = LedgerTest(
    "CSsubmitAndWaitForTransactionId",
    "SubmitAndWaitForTransactionId returns a valid transaction identifier") { context =>
    for {
      ledger <- context.participant()
      alice <- ledger.allocateParty()
      request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      transactionId <- ledger.submitAndWaitForTransactionId(request)
      retrievedTransaction <- ledger.transactionTreeById(transactionId, alice)
      transactions <- ledger.flatTransactions(alice)
    } yield {

      assert(transactionId.nonEmpty, "The transaction identifier was empty but shouldn't.")
      assert(
        transactions.size == 1,
        s"$alice should see only one transaction but sees ${transactions.size}")
      val events = transactions.head.events

      assert(events.size == 1, s"$alice should see only one event but sees ${events.size}")
      assert(
        events.head.event.isCreated,
        s"$alice should see only one create but sees ${events.head.event}")
      val created = transactions.head.events.head.getCreated

      assert(
        retrievedTransaction.transactionId == transactionId,
        s"$alice should see the transaction for the created contract $transactionId but sees ${retrievedTransaction.transactionId}"
      )
      assert(
        retrievedTransaction.rootEventIds.size == 1,
        s"The retrieved transaction should contain a single event but contains ${retrievedTransaction.rootEventIds.size}"
      )
      val retrievedEvent = retrievedTransaction.eventsById(retrievedTransaction.rootEventIds.head)

      assert(
        retrievedEvent.kind.isCreated,
        s"The only event seen should be a created but instead it's $retrievedEvent")
      assert(
        retrievedEvent.getCreated == created,
        s"The retrieved created event does not match the one in the flat transactions: event=$created retrieved=$retrievedEvent"
      )

    }
  }

  private val submitAndWaitForTransactionTest = LedgerTest(
    "CSsubmitAndWaitForTransaction",
    "SubmitAndWaitForTransaction returns a transaction") { context =>
    for {
      ledger <- context.participant()
      alice <- ledger.allocateParty()
      request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      transaction <- ledger.submitAndWaitForTransaction(request)
    } yield {
      assert(
        transaction.transactionId.nonEmpty,
        "The transaction identifier was empty but shouldn't.")
      assert(
        transaction.events.size == 1,
        s"The returned transaction should contain 1 event, but contained ${transaction.events.size}")
      val event = transaction.events.head
      assert(
        event.event.isCreated,
        s"The returned transaction should contain a created-event, but was ${event.event}"
      )
      assert(
        event.getCreated.getTemplateId == Dummy.id.unwrap,
        s"The template ID of the created-event should by ${Dummy.id.unwrap}, but was ${event.getCreated.getTemplateId}"
      )
    }
  }

  private val submitAndWaitForTransactionTreeTest = LedgerTest(
    "CSsubmitAndWaitForTransactionTree",
    "SubmitAndWaitForTransactionTree returns a transaction tree") { context =>
    for {
      ledger <- context.participant()
      alice <- ledger.allocateParty()
      request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      transactionTree <- ledger.submitAndWaitForTransactionTree(request)
    } yield {
      assert(
        transactionTree.transactionId.nonEmpty,
        "The transaction identifier was empty but shouldn't.")
      assert(
        transactionTree.eventsById.size == 1,
        s"The returned transaction tree should contain 1 event, but contained ${transactionTree.eventsById.size}")
      val event = transactionTree.eventsById.head._2
      assert(
        event.kind.isCreated,
        s"The returned transaction tree should contain a created-event, but was ${event.kind}")
      assert(
        event.getCreated.getTemplateId == Dummy.id.unwrap,
        s"The template ID of the created-event should by ${Dummy.id.unwrap}, but was ${event.getCreated.getTemplateId}"
      )
    }
  }

  private val resendingSubmitAndWait = LedgerTest(
    "CSduplicateSubmitAndWait",
    "SubmitAndWait should be idempotent when reusing the same command identifier") { context =>
    for {
      ledger <- context.participant()
      alice <- ledger.allocateParty()
      request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      _ <- ledger.submitAndWait(request)
      _ <- ledger.submitAndWait(request)
      transactions <- ledger.flatTransactions(alice)
    } yield {
      assert(
        transactions.size == 1,
        s"Expected only 1 transaction, but received ${transactions.size}")

    }
  }

  private val resendingSubmitAndWaitForTransactionId = LedgerTest(
    "CSduplicateSubmitAndWaitForTransactionId",
    "SubmitAndWaitForTransactionId should be idempotent when reusing the same command identifier") {
    context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
        transactionId1 <- ledger.submitAndWaitForTransactionId(request)
        transactionId2 <- ledger.submitAndWaitForTransactionId(request)
      } yield {
        assert(
          transactionId1 == transactionId2,
          s"The transaction identifiers did not match: transactionId1=$transactionId1, transactionId2=$transactionId2")
      }
  }

  private val resendingSubmitAndWaitForTransaction = LedgerTest(
    "CSduplicateSubmitAndWaitForTransaction",
    "SubmitAndWaitForTransaction should be idempotent when reusing the same command identifier") {
    context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
        transaction1 <- ledger.submitAndWaitForTransaction(request)
        transaction2 <- ledger.submitAndWaitForTransaction(request)
      } yield {
        assert(
          transaction1 == transaction2,
          s"The transactions did not match: transaction1=$transaction1, transaction2=$transaction2")
      }
  }

  private val resendingSubmitAndWaitForTransactionTree = LedgerTest(
    "CSduplicateSubmitAndWaitForTransactionTree",
    "SubmitAndWaitForTransactionTree should be idempotent when reusing the same command identifier") {
    context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
        transactionTree1 <- ledger.submitAndWaitForTransactionTree(request)
        transactionTree2 <- ledger.submitAndWaitForTransactionTree(request)
      } yield {
        assert(
          transactionTree1 == transactionTree2,
          s"The transaction trees did not match: transactionTree1=$transactionTree1, transactionTree2=$transactionTree2")
      }
  }

  private val submitAndWaitWithInvalidLedgerIdTest = LedgerTest(
    "CSsubmitAndWaitInvalidLedgerId",
    "SubmitAndWait should fail for invalid ledger ids") { context =>
    val invalidLedgerId = "CSsubmitAndWaitInvalidLedgerId"
    for {
      ledger <- context.participant()
      alice <- ledger.allocateParty()
      request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
      failure <- ledger.submitAndWait(badLedgerId).failed
    } yield
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
  }

  private val submitAndWaitForTransactionIdWithInvalidLedgerIdTest = LedgerTest(
    "CSsubmitAndWaitForTransactionIdInvalidLedgerId",
    "SubmitAndWaitForTransactionId should fail for invalid ledger ids") { context =>
    val invalidLedgerId = "CSsubmitAndWaitForTransactionIdInvalidLedgerId"
    for {
      ledger <- context.participant()
      alice <- ledger.allocateParty()
      request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
      failure <- ledger.submitAndWaitForTransactionId(badLedgerId).failed
    } yield
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
  }

  private val submitAndWaitForTransactionWithInvalidLedgerIdTest = LedgerTest(
    "CSsubmitAndWaitForTransactionInvalidLedgerId",
    "SubmitAndWaitForTransaction should fail for invalid ledger ids") { context =>
    val invalidLedgerId = "CSsubmitAndWaitForTransactionInvalidLedgerId"
    for {
      ledger <- context.participant()
      alice <- ledger.allocateParty()
      request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
      failure <- ledger.submitAndWaitForTransaction(badLedgerId).failed
    } yield
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
  }

  private val submitAndWaitForTransactionTreeWithInvalidLedgerIdTest = LedgerTest(
    "CSsubmitAndWaitForTransactionTreeInvalidLedgerId",
    "SubmitAndWaitForTransactionTree should fail for invalid ledger ids") { context =>
    val invalidLedgerId = "CSsubmitAndWaitForTransactionTreeInvalidLedgerId"
    for {
      ledger <- context.participant()
      alice <- ledger.allocateParty()
      request <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
      badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
      failure <- ledger.submitAndWaitForTransactionTree(badLedgerId).failed
    } yield
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
  }

  override val tests: Vector[LedgerTest] = Vector(
    submitAndWaitTest,
    submitAndWaitForTransactionTest,
    submitAndWaitForTransactionIdTest,
    submitAndWaitForTransactionTreeTest,
    resendingSubmitAndWait,
    resendingSubmitAndWaitForTransaction,
    resendingSubmitAndWaitForTransactionId,
    resendingSubmitAndWaitForTransactionTree,
    submitAndWaitWithInvalidLedgerIdTest,
    submitAndWaitForTransactionIdWithInvalidLedgerIdTest,
    submitAndWaitForTransactionWithInvalidLedgerIdTest,
    submitAndWaitForTransactionTreeWithInvalidLedgerIdTest
  )
}
