// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.{
  LedgerSession,
  LedgerTest,
  LedgerTestContext,
  LedgerTestSuite
}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.test.Test.Dummy
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Status
import scalaz.syntax.tag._

import scala.concurrent.Future

final class CommandService(session: LedgerSession) extends LedgerTestSuite(session) {
  private val submitAndWait = LedgerTest("CSsubmitAndWait", "SubmitAndWait returns empty") {
    implicit context =>
      for {
        alice <- allocateParty()
        empty <- session.bindings.submitAndWait(
          alice,
          context.applicationId,
          context.nextCommandId(),
          Seq(Dummy(alice).create.command.command))
      } yield {
        assert(empty == Empty())
      }
  }

  private val submitAndWaitForTransactionId = LedgerTest(
    "CSsubmitAndWaitForTransactionId",
    "SubmitAndWaitForTransactionId returns a transaction id") { implicit context =>
    for {
      alice <- allocateParty()
      transactionId <- session.bindings.submitAndWaitForTransactionId(
        alice,
        context.applicationId,
        context.nextCommandId(),
        Seq(Dummy(alice).create.command.command))
    } yield {
      assert(
        transactionId.nonEmpty,
        s"The returned transaction ID should be a non empty string, but was '$transactionId'.")
    }
  }

  private val submitAndWaitForTransaction = LedgerTest(
    "CSsubmitAndWaitForTransaction",
    "SubmitAndWaitForTransaction returns a transaction") { implicit context =>
    for {
      alice <- allocateParty()
      transaction <- session.bindings.submitAndWaitForTransaction(
        alice,
        context.applicationId,
        context.nextCommandId(),
        Seq(Dummy(alice).create.command.command))
    } yield {
      assert(
        transaction.transactionId.nonEmpty,
        s"The returned transaction ID should be a non empty string, but was '${transaction.transactionId}'.")

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
        s"The template ID of the created-event should by ${Dummy.id}, but was ${event.getCreated.getTemplateId}"
      )
    }
  }

  private val submitAndWaitForTransactionTree = LedgerTest(
    "CSsubmitAndWaitForTransactionTree",
    "SubmitAndWaitForTransactionTree returns a transaction tree") { implicit context =>
    for {
      alice <- allocateParty()
      transactionTree <- session.bindings.submitAndWaitForTransactionTree(
        alice,
        context.applicationId,
        context.nextCommandId(),
        Seq(Dummy(alice).create.command.command))
    } yield {
      assert(
        transactionTree.transactionId.nonEmpty,
        s"The returned transaction ID should be a non empty string, but was '${transactionTree.transactionId}'.")

      assert(
        transactionTree.eventsById.size == 1,
        s"The returned transaction tree should contain 1 event, but contained ${transactionTree.eventsById.size}")

      val event = transactionTree.eventsById.head._2
      assert(
        event.kind.isCreated,
        s"The returned transaction tree should contain a created-event, but was ${event.kind}")

      assert(
        event.getCreated.getTemplateId == Dummy.id.unwrap,
        s"The template ID of the created-event should by ${Dummy.id}, but was ${event.getCreated.getTemplateId}"
      )
    }
  }

  private val resendingSubmitAndWait = LedgerTest(
    "CSduplicateSubmitAndWait",
    "SubmitAndWait returns Empty for a duplicate submission") { implicit context =>
    for {
      alice <- allocateParty()
      commandId = context.nextCommandId()
      empty1 <- session.bindings.submitAndWait(
        alice,
        context.applicationId,
        commandId,
        Seq(Dummy(alice).create.command.command))
      empty2 <- session.bindings.submitAndWait(
        alice,
        context.applicationId,
        commandId,
        Seq(Dummy(alice).create.command.command))
      transactions <- flatTransactions(alice)
    } yield {
      assert(empty1 == empty2, "The responses of SubmitAndWait did not match.")
      assert(
        transactions.size == 1,
        s"Expected only 1 transaction, but received ${transactions.size}")

    }
  }

  private val resendingSubmitAndWaitForTransactionId = LedgerTest(
    "CSduplicateSubmitAndWaitForTransactionId",
    "SubmitAndWaitForTransactionId returns the same transaction ID for a duplicate submission") {
    implicit context =>
      for {
        alice <- allocateParty()
        commandId = context.nextCommandId()
        transactionId1 <- session.bindings.submitAndWaitForTransactionId(
          alice,
          context.applicationId,
          commandId,
          Seq(Dummy(alice).create.command.command))
        transactionId2 <- session.bindings.submitAndWaitForTransactionId(
          alice,
          context.applicationId,
          commandId,
          Seq(Dummy(alice).create.command.command))
      } yield {
        assert(
          transactionId1 == transactionId2,
          s"The transaction IDs did not match: transactionId1=$transactionId1, transactionId2=$transactionId2")
      }
  }

  private val resendingSubmitAndWaitForTransaction = LedgerTest(
    "CSduplicateSubmitAndWaitForTransaction",
    "SubmitAndWaitForTransaction returns the same transaction for a duplicate submission") {
    implicit context =>
      for {
        alice <- allocateParty()
        commandId = context.nextCommandId()
        transaction1 <- session.bindings.submitAndWaitForTransaction(
          alice,
          context.applicationId,
          commandId,
          Seq(Dummy(alice).create.command.command))
        transaction2 <- session.bindings.submitAndWaitForTransaction(
          alice,
          context.applicationId,
          commandId,
          Seq(Dummy(alice).create.command.command))
      } yield {
        assert(
          transaction1 == transaction2,
          s"The transaction did not match: transaction1=$transaction1, transaction2=$transaction2")
      }
  }

  private val resendingSubmitAndWaitForTransactionTree = LedgerTest(
    "CSduplicateSubmitAndWaitForTransactionTree",
    "SubmitAndWaitForTransactionTree returns the same transaction for a duplicate submission") {
    implicit context =>
      for {
        alice <- allocateParty()
        commandId = context.nextCommandId()
        transactionTree1 <- session.bindings.submitAndWaitForTransactionTree(
          alice,
          context.applicationId,
          commandId,
          Seq(Dummy(alice).create.command.command))
        transactionTree2 <- session.bindings.submitAndWaitForTransactionTree(
          alice,
          context.applicationId,
          commandId,
          Seq(Dummy(alice).create.command.command))
      } yield {
        assert(
          transactionTree1 == transactionTree2,
          s"The transaction trees did not match: transactionTree1=$transactionTree1, transactionTree2=$transactionTree2")
      }
  }

  private def invalidLedgerIdTest[A](method: SubmitAndWaitRequest => Future[A])(
      implicit context: LedgerTestContext) =
    for {
      party <- allocateParty()
      invalidLedgerId = s"some-wrong-ledger-id-${UUID.randomUUID}"
      let <- context.time
      mrt = let.plusSeconds((30 * session.config.commandTtlFactor).toLong)
      request = SubmitAndWaitRequest(
        Some(
          Commands(
            ledgerId = invalidLedgerId,
            workflowId = "",
            applicationId = context.applicationId,
            commandId = context.nextCommandId(),
            party = party.unwrap,
            ledgerEffectiveTime = Some(Timestamp(let.getEpochSecond, let.getNano)),
            maximumRecordTime = Some(Timestamp(mrt.getEpochSecond, mrt.getNano)),
            Seq(Dummy(party).create.command)
          )))
      failure <- method(request).failed
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '${invalidLedgerId}' not found.")
    }

  private val submitAndWaitWithInvalidLedgerId = LedgerTest(
    "CSsubmitAndWaitInvalidLedgerId",
    "SubmitAndWait should fail for invalid ledger ids") { implicit context =>
    invalidLedgerIdTest(session.services.command.submitAndWait)
  }

  private val submitAndWaitForTransactionIdWithInvalidLedgerId = LedgerTest(
    "CSsubmitAndWaitForTransactionIdInvalidLedgerId",
    "SubmitAndWaitForTransactionId should fail for invalid ledger ids") { implicit context =>
    invalidLedgerIdTest(session.services.command.submitAndWaitForTransactionId)
  }

  private val submitAndWaitForTransactionWithInvalidLedgerId = LedgerTest(
    "CSsubmitAndWaitForTransactionInvalidLedgerId",
    "SubmitAndWaitForTransaction should fail for invalid ledger ids") { implicit context =>
    invalidLedgerIdTest(session.services.command.submitAndWaitForTransaction)
  }

  private val submitAndWaitForTransactionTreeWithInvalidLedgerId = LedgerTest(
    "CSsubmitAndWaitForTransactionTreeInvalidLedgerId",
    "SubmitAndWaitForTransactionTree should fail for invalid ledger ids") { implicit context =>
    invalidLedgerIdTest(session.services.command.submitAndWaitForTransactionTree)
  }

  override val tests: Vector[LedgerTest] = Vector(
    submitAndWait,
    submitAndWaitForTransaction,
    submitAndWaitForTransactionId,
    submitAndWaitForTransactionTree,
    resendingSubmitAndWait,
    resendingSubmitAndWaitForTransaction,
    resendingSubmitAndWaitForTransactionId,
    resendingSubmitAndWaitForTransactionTree,
    submitAndWaitWithInvalidLedgerId,
    submitAndWaitForTransactionIdWithInvalidLedgerId,
    submitAndWaitForTransactionWithInvalidLedgerId,
    submitAndWaitForTransactionTreeWithInvalidLedgerId
  )
}
