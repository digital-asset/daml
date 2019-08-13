package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test.Test.Dummy
//import com.digitalasset.ledger.test.Test.Dummy._
import com.google.protobuf.empty.Empty
import scalaz.syntax.tag._

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

//  private val submitAndWaitWithInvalidLedgerId = LedgerTest(
//    "CSsubmitAndWaitInvalidLedgerId",
//    "SubmitAndWait should fail for invalid ledger ids") { implicit context =>
//    for {
//      alice <- allocateParty()
//      _ <- session.bindings.submitAndWait()
//    } yield {}
//  }

  override val tests: Vector[LedgerTest] = Vector(
    submitAndWait,
    submitAndWaitForTransaction,
    submitAndWaitForTransactionId,
    submitAndWaitForTransactionTree,
    resendingSubmitAndWait,
    resendingSubmitAndWaitForTransaction,
    resendingSubmitAndWaitForTransactionId,
    resendingSubmitAndWaitForTransactionTree,
  )
}
