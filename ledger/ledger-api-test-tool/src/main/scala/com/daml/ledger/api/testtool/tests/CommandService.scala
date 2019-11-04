// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.util.UUID

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.ledger.client.binding.Primitive
import com.digitalasset.ledger.client.binding.Value.encode
import com.digitalasset.ledger.test_stable.Test.CallablePayout._
import com.digitalasset.ledger.test_stable.Test.Dummy._
import com.digitalasset.ledger.test_stable.Test.DummyFactory._
import com.digitalasset.ledger.test_stable.Test.WithObservers._
import com.digitalasset.ledger.test_stable.Test._
import com.digitalasset.platform.testing.{TimeoutException, WithTimeout}
import io.grpc.Status
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt

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
      request <- ledger.submitRequest(alice, Dummy(alice).create.command)
      badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
      failure <- ledger.submit(badLedgerId).failed
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

  private[this] val disallowEmptyCommandSubmission = LedgerTest(
    "CSDisallowEmptyTransactionsSubmission",
    "The submission of an empty command should be rejected with INVALID_ARGUMENT"
  ) { context =>
    for {
      ledger <- context.participant()
      party <- ledger.allocateParty()
      emptyRequest <- ledger.submitRequest(party)
      failure <- ledger.submit(emptyRequest).failed
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Missing field: commands")
    }
  }

  private[this] val refuseBadChoice = LedgerTest(
    "CSRefuseBadChoice",
    "The submission of an exercise of a choice that does not exist should yield INVALID_ARGUMENT"
  ) { context =>
    val badChoice = "THIS_IS_NOT_A_VALID_CHOICE"
    for {
      ledger <- context.participant()
      party <- ledger.allocateParty()
      dummy <- ledger.create(party, Dummy(party))
      exercise = dummy.exerciseDummyChoice1(party).command
      wrongExercise = exercise.update(_.exercise.choice := badChoice)
      wrongRequest <- ledger.submitRequest(party, wrongExercise)
      failure <- ledger.submit(wrongRequest).failed
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        s"Couldn't find requested choice $badChoice")
    }
  }

  private[this] val returnStackTrace = LedgerTest(
    "CSReturnStackTrace",
    "A submission resulting in an interpretation error should return the stack trace"
  ) { context =>
    for {
      ledger <- context.participant()
      party <- ledger.allocateParty()
      dummy <- ledger.create(party, Dummy(party))
      failure <- ledger.exercise(party, dummy.exerciseFailingClone).failed
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        "Command interpretation error in LF-DAMLe: Interpretation error: Error: User abort: Assertion failed. Details: Last location: [DA.Internal.Assert:20], partial transaction: root node"
      )
    }
  }

  private[this] val exerciseByKey = LedgerTest(
    "CSExerciseByKey",
    "Exercising by key should be possible only when the corresponding contract is available"
  ) { context =>
    val keyString = UUID.randomUUID.toString
    for {
      ledger <- context.participant()
      party <- ledger.allocateParty()
      expectedKey = Value(
        Value.Sum.Record(
          Record(
            fields = Seq(
              RecordField("_1", Some(Value(Value.Sum.Party(party.unwrap)))),
              RecordField("_2", Some(Value(Value.Sum.Text(keyString))))
            ))))
      failureBeforeCreation <- ledger
        .exerciseByKey(
          party,
          TextKey.id,
          expectedKey,
          "TextKeyChoice",
          Value(Value.Sum.Record(Record())))
        .failed
      _ <- ledger.create(party, TextKey(party, keyString, Primitive.List.empty))
      _ <- ledger.exerciseByKey(
        party,
        TextKey.id,
        expectedKey,
        "TextKeyChoice",
        Value(Value.Sum.Record(Record())))
      failureAfterConsuming <- ledger
        .exerciseByKey(
          party,
          TextKey.id,
          expectedKey,
          "TextKeyChoice",
          Value(Value.Sum.Record(Record())))
        .failed
    } yield {
      assertGrpcError(
        failureBeforeCreation,
        Status.Code.INVALID_ARGUMENT,
        "dependency error: couldn't find key"
      )
      assertGrpcError(
        failureAfterConsuming,
        Status.Code.INVALID_ARGUMENT,
        "dependency error: couldn't find key"
      )
    }
  }

  private[this] val discloseCreateToObservers =
    LedgerTest("CSDiscloseCreateToObservers", "Disclose create to observers") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        Vector(giver, observer1) <- alpha.allocateParties(2)
        observer2 <- beta.allocateParty()
        template = WithObservers(giver, Primitive.List(observer1, observer2))
        _ <- alpha.create(giver, template)
        observer1View <- alpha.transactionTrees(observer1)
        observer2View <- beta.transactionTrees(observer2)
      } yield {
        val observer1Created = assertSingleton(
          "The first observer should see exactly one creation",
          observer1View.flatMap(createdEvents))
        val observer2Created = assertSingleton(
          "The second observer should see exactly one creation",
          observer2View.flatMap(createdEvents))
        assertEquals(
          "The two observers should see the same creation",
          observer1Created.getCreateArguments.fields,
          observer2Created.getCreateArguments.fields)
        assertEquals(
          "The observers shouls see the created contract",
          observer1Created.getCreateArguments.fields,
          encode(template).getRecord.fields
        )
      }
    }

  private[this] val discloseExerciseToObservers =
    LedgerTest("CSDiscloseExerciseToObservers", "Diclose exercise to observers") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        Vector(giver, observer1) <- alpha.allocateParties(2)
        observer2 <- beta.allocateParty()
        template = WithObservers(giver, Primitive.List(observer1, observer2))
        withObservers <- alpha.create(giver, template)
        _ <- alpha.exercise(giver, withObservers.exercisePing)
        observer1View <- alpha.transactionTrees(observer1)
        observer2View <- beta.transactionTrees(observer2)
      } yield {
        val observer1Exercise = assertSingleton(
          "The first observer should see exactly one exercise",
          observer1View.flatMap(exercisedEvents))
        val observer2Exercise = assertSingleton(
          "The second observer should see exactly one exercise",
          observer2View.flatMap(exercisedEvents))
        assert(
          observer1Exercise.contractId == observer2Exercise.contractId,
          "The two observers should see the same exercise")
        assert(
          observer1Exercise.contractId == withObservers.unwrap,
          "The observers shouls see the exercised contract")
      }
    }

  private[this] val hugeCommandSubmission =
    LedgerTest("CSHugeCommandSubmittion", "The server should accept a submission with 15 commands") {
      context =>
        {
          val target = 15
          for {
            ledger <- context.participant()
            party <- ledger.allocateParty()
            commands = Vector.fill(target)(Dummy(party).create.command)
            request <- ledger.submitAndWaitRequest(party, commands: _*)
            _ <- ledger.submitAndWait(request)
            acs <- ledger.activeContracts(party)
          } yield {
            assert(
              acs.size == target,
              s"Expected $target contracts to be created, got ${acs.size} instead")
          }
        }
    }

  private[this] val callablePayout =
    LedgerTest("CSCallablePayout", "Run CallablePayout and return the right events") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        Vector(giver, newReceiver) <- alpha.allocateParties(2)
        receiver <- beta.allocateParty()
        callablePayout <- alpha.create(giver, CallablePayout(giver, receiver))
        tree <- beta.exercise(receiver, callablePayout.exerciseTransfer(_, newReceiver))
      } yield {
        val created = assertSingleton("There should only be one creation", createdEvents(tree))
        assertEquals(
          "The created event should be the expected one",
          created.getCreateArguments.fields,
          encode(CallablePayout(giver, newReceiver)).getRecord.fields)
      }
    }

  private[this] val readyForExercise =
    LedgerTest(
      "CSReadyForExercise",
      "It should be possible to exercise a choice on a created contract") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        factory <- ledger.create(party, DummyFactory(party))
        tree <- ledger.exercise(party, factory.exerciseDummyFactoryCall)
      } yield {
        val exercise = assertSingleton("There should only be one exercise", exercisedEvents(tree))
        assert(exercise.contractId == factory.unwrap, "Contract identifier mismatch")
        assert(exercise.consuming, "The choice should have been consuming")
        val _ = assertLength("Two creations should have occured", 2, createdEvents(tree))
      }
    }

  private[this] val completions =
    LedgerTest(
      "CSCompletions",
      "Read completions correctly with a correct application identifier and reading party") {
      context =>
        {
          for {
            ledger <- context.participant()
            party <- ledger.allocateParty()
            request <- ledger.submitRequest(party, Dummy(party).create.command)
            _ <- ledger.submit(request)
            completions <- ledger.firstCompletions(party)
          } yield {
            val commandId =
              assertSingleton("Expected only one completion", completions.map(_.commandId))
            assert(
              commandId == request.commands.get.commandId,
              "Wrong command identifier on completion")
          }
        }
    }

  private[this] val noCompletionsWithoutRightAppId =
    LedgerTest(
      "CSNoCompletionsWithoutRightAppId",
      "Read no completions without the correct application identifier") { context =>
      {
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          request <- ledger.submitRequest(party, Dummy(party).create.command)
          _ <- ledger.submit(request)
          invalidRequest = ledger
            .completionStreamRequest(party)
            .update(_.applicationId := "invalid-application-id")
          failed <- WithTimeout(5.seconds)(ledger.firstCompletions(invalidRequest)).failed
        } yield {
          assert(failed == TimeoutException, "Timeout expected")
        }
      }
    }

  private[this] val noCompletionsWithoutRightParty =
    LedgerTest("CSNoCompletionsWithoutRightParty", "Read no completions without the correct party") {
      context =>
        {
          for {
            ledger <- context.participant()
            Vector(party, notTheSubmittingParty) <- ledger.allocateParties(2)
            request <- ledger.submitRequest(party, Dummy(party).create.command)
            _ <- ledger.submit(request)
            failed <- WithTimeout(5.seconds)(ledger.firstCompletions(notTheSubmittingParty)).failed
          } yield {
            assert(failed == TimeoutException, "Timeout expected")
          }
        }
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
    submitAndWaitForTransactionTreeWithInvalidLedgerIdTest,
    disallowEmptyCommandSubmission,
    refuseBadChoice,
    returnStackTrace,
    exerciseByKey,
    discloseCreateToObservers,
    discloseExerciseToObservers,
    hugeCommandSubmission,
    callablePayout,
    readyForExercise,
    completions,
    noCompletionsWithoutRightAppId,
    noCompletionsWithoutRightParty
  )
}
