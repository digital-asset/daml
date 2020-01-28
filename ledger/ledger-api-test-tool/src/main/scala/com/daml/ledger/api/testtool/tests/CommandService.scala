// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.commands.Command
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.ledger.client.binding.Primitive
import com.digitalasset.ledger.client.binding.Value.encode
import com.digitalasset.ledger.test_stable.Test.CallablePayout._
import com.digitalasset.ledger.test_stable.Test.Dummy._
import com.digitalasset.ledger.test_stable.Test.DummyFactory._
import com.digitalasset.ledger.test_stable.Test.WithObservers._
import com.digitalasset.ledger.test_stable.Test.{Dummy, _}
import io.grpc.Status
import scalaz.syntax.tag._

final class CommandService(session: LedgerSession) extends LedgerTestSuite(session) {
  test(
    "CSsubmitAndWait",
    "SubmitAndWait creates a contract of the expected template",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        _ <- ledger.submitAndWait(request)
        active <- ledger.activeContracts(party)
      } yield {
        assert(active.size == 1)
        val dummyTemplateId = active.flatMap(_.templateId.toList).head
        assert(dummyTemplateId == Dummy.id.unwrap)
      }
  }

  test(
    "CSsubmitAndWaitForTransactionId",
    "SubmitAndWaitForTransactionId returns a valid transaction identifier",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        transactionId <- ledger.submitAndWaitForTransactionId(request)
        retrievedTransaction <- ledger.transactionTreeById(transactionId, party)
        transactions <- ledger.flatTransactions(party)
      } yield {

        assert(transactionId.nonEmpty, "The transaction identifier was empty but shouldn't.")
        assert(
          transactions.size == 1,
          s"$party should see only one transaction but sees ${transactions.size}",
        )
        val events = transactions.head.events

        assert(events.size == 1, s"$party should see only one event but sees ${events.size}")
        assert(
          events.head.event.isCreated,
          s"$party should see only one create but sees ${events.head.event}",
        )
        val created = transactions.head.events.head.getCreated

        assert(
          retrievedTransaction.transactionId == transactionId,
          s"$party should see the transaction for the created contract $transactionId but sees ${retrievedTransaction.transactionId}",
        )
        assert(
          retrievedTransaction.rootEventIds.size == 1,
          s"The retrieved transaction should contain a single event but contains ${retrievedTransaction.rootEventIds.size}",
        )
        val retrievedEvent = retrievedTransaction.eventsById(retrievedTransaction.rootEventIds.head)

        assert(
          retrievedEvent.kind.isCreated,
          s"The only event seen should be a created but instead it's $retrievedEvent",
        )
        assert(
          retrievedEvent.getCreated == created,
          s"The retrieved created event does not match the one in the flat transactions: event=$created retrieved=$retrievedEvent",
        )

      }
  }

  test(
    "CSsubmitAndWaitForTransaction",
    "SubmitAndWaitForTransaction returns a transaction",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        transaction <- ledger.submitAndWaitForTransaction(request)
      } yield {
        assert(
          transaction.transactionId.nonEmpty,
          "The transaction identifier was empty but shouldn't.",
        )
        assert(
          transaction.events.size == 1,
          s"The returned transaction should contain 1 event, but contained ${transaction.events.size}",
        )
        val event = transaction.events.head
        assert(
          event.event.isCreated,
          s"The returned transaction should contain a created-event, but was ${event.event}",
        )
        assert(
          event.getCreated.getTemplateId == Dummy.id.unwrap,
          s"The template ID of the created-event should by ${Dummy.id.unwrap}, but was ${event.getCreated.getTemplateId}",
        )
      }
  }

  test(
    "CSsubmitAndWaitForTransactionTree",
    "SubmitAndWaitForTransactionTree returns a transaction tree",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        transactionTree <- ledger.submitAndWaitForTransactionTree(request)
      } yield {
        assert(
          transactionTree.transactionId.nonEmpty,
          "The transaction identifier was empty but shouldn't.",
        )
        assert(
          transactionTree.eventsById.size == 1,
          s"The returned transaction tree should contain 1 event, but contained ${transactionTree.eventsById.size}",
        )
        val event = transactionTree.eventsById.head._2
        assert(
          event.kind.isCreated,
          s"The returned transaction tree should contain a created-event, but was ${event.kind}",
        )
        assert(
          event.getCreated.getTemplateId == Dummy.id.unwrap,
          s"The template ID of the created-event should by ${Dummy.id.unwrap}, but was ${event.getCreated.getTemplateId}",
        )
      }
  }

  test(
    "CSduplicateSubmitAndWait",
    "SubmitAndWait should be idempotent when reusing the same command identifier",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        _ <- ledger.submitAndWait(request)
        _ <- ledger.submitAndWait(request)
        transactions <- ledger.flatTransactions(party)
      } yield {
        assert(
          transactions.size == 1,
          s"Expected only 1 transaction, but received ${transactions.size}",
        )

      }
  }

  test(
    "CSduplicateSubmitAndWaitForTransactionId",
    "SubmitAndWaitForTransactionId should be idempotent when reusing the same command identifier",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        transactionId1 <- ledger.submitAndWaitForTransactionId(request)
        transactionId2 <- ledger.submitAndWaitForTransactionId(request)
      } yield {
        assert(
          transactionId1 == transactionId2,
          s"The transaction identifiers did not match: transactionId1=$transactionId1, transactionId2=$transactionId2",
        )
      }
  }

  test(
    "CSduplicateSubmitAndWaitForTransaction",
    "SubmitAndWaitForTransaction should be idempotent when reusing the same command identifier",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        transaction1 <- ledger.submitAndWaitForTransaction(request)
        transaction2 <- ledger.submitAndWaitForTransaction(request)
      } yield {
        assert(
          transaction1 == transaction2,
          s"The transactions did not match: transaction1=$transaction1, transaction2=$transaction2",
        )
      }
  }

  test(
    "CSduplicateSubmitAndWaitForTransactionTree",
    "SubmitAndWaitForTransactionTree should be idempotent when reusing the same command identifier",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        transactionTree1 <- ledger.submitAndWaitForTransactionTree(request)
        transactionTree2 <- ledger.submitAndWaitForTransactionTree(request)
      } yield {
        assert(
          transactionTree1 == transactionTree2,
          s"The transaction trees did not match: transactionTree1=$transactionTree1, transactionTree2=$transactionTree2",
        )
      }
  }

  test(
    "CSsubmitAndWaitForTransactionIdInvalidLedgerId",
    "SubmitAndWaitForTransactionId should fail for invalid ledger ids",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val invalidLedgerId = "CSsubmitAndWaitForTransactionIdInvalidLedgerId"
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
        failure <- ledger.submitAndWaitForTransactionId(badLedgerId).failed
      } yield assertGrpcError(
        failure,
        Status.Code.NOT_FOUND,
        s"Ledger ID '$invalidLedgerId' not found.",
      )
  }

  test(
    "CSsubmitAndWaitForTransactionInvalidLedgerId",
    "SubmitAndWaitForTransaction should fail for invalid ledger ids",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val invalidLedgerId = "CSsubmitAndWaitForTransactionInvalidLedgerId"
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
        failure <- ledger.submitAndWaitForTransaction(badLedgerId).failed
      } yield assertGrpcError(
        failure,
        Status.Code.NOT_FOUND,
        s"Ledger ID '$invalidLedgerId' not found.",
      )
  }

  test(
    "CSsubmitAndWaitForTransactionTreeInvalidLedgerId",
    "SubmitAndWaitForTransactionTree should fail for invalid ledger ids",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val invalidLedgerId = "CSsubmitAndWaitForTransactionTreeInvalidLedgerId"
      for {
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        badLedgerId = request.update(_.commands.ledgerId := invalidLedgerId)
        failure <- ledger.submitAndWaitForTransactionTree(badLedgerId).failed
      } yield assertGrpcError(
        failure,
        Status.Code.NOT_FOUND,
        s"Ledger ID '$invalidLedgerId' not found.",
      )
  }

  test(
    "CSRefuseBadParameter",
    "The submission of a creation that contains a bad parameter label should result in an INVALID_ARGUMENT",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val createWithBadArgument = Dummy(party).create.command
        .update(_.create.createArguments.fields.foreach(_.label := "INVALID_PARAM"))
      for {
        badRequest <- ledger.submitAndWaitRequest(party, createWithBadArgument)
        failure <- ledger.submitAndWait(badRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, s"Missing record label")
      }
  }

  test(
    "CSReturnStackTrace",
    "A submission resulting in an interpretation error should return the stack trace",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        dummy <- ledger.create(party, Dummy(party))
        failure <- ledger.exercise(party, dummy.exerciseFailingClone).failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          "Command interpretation error in LF-DAMLe: Interpretation error: Error: User abort: Assertion failed. Details: Last location: [DA.Internal.Assert:20], partial transaction: root node",
        )
      }
  }

  test(
    "CSDiscloseCreateToObservers",
    "Disclose create to observers",
    allocate(TwoParties, SingleParty),
  ) {
    case Participants(Participant(alpha, giver, observer1), Participant(beta, observer2)) =>
      val template = WithObservers(giver, Primitive.List(observer1, observer2))
      for {
        _ <- alpha.create(giver, template)
        _ <- synchronize(alpha, beta)
        observer1View <- alpha.transactionTrees(observer1)
        observer2View <- beta.transactionTrees(observer2)
      } yield {
        val observer1Created = assertSingleton(
          "The first observer should see exactly one creation",
          observer1View.flatMap(createdEvents),
        )
        val observer2Created = assertSingleton(
          "The second observer should see exactly one creation",
          observer2View.flatMap(createdEvents),
        )
        assertEquals(
          "The two observers should see the same creation",
          observer1Created.getCreateArguments.fields,
          observer2Created.getCreateArguments.fields,
        )
        assertEquals(
          "The observers should see the created contract",
          observer1Created.getCreateArguments.fields,
          encode(template).getRecord.fields,
        )
      }
  }

  test(
    "CSDiscloseExerciseToObservers",
    "Disclose exercise to observers",
    allocate(TwoParties, SingleParty),
  ) {
    case Participants(Participant(alpha, giver, observer1), Participant(beta, observer2)) =>
      val template = WithObservers(giver, Primitive.List(observer1, observer2))
      for {
        withObservers <- alpha.create(giver, template)
        _ <- alpha.exercise(giver, withObservers.exercisePing)
        _ <- synchronize(alpha, beta)
        observer1View <- alpha.transactionTrees(observer1)
        observer2View <- beta.transactionTrees(observer2)
      } yield {
        val observer1Exercise = assertSingleton(
          "The first observer should see exactly one exercise",
          observer1View.flatMap(exercisedEvents),
        )
        val observer2Exercise = assertSingleton(
          "The second observer should see exactly one exercise",
          observer2View.flatMap(exercisedEvents),
        )
        assert(
          observer1Exercise.contractId == observer2Exercise.contractId,
          "The two observers should see the same exercise",
        )
        assert(
          observer1Exercise.contractId == withObservers.unwrap,
          "The observers shouls see the exercised contract",
        )
      }
  }

  test(
    "CSHugeCommandSubmission",
    "The server should accept a submission with 15 commands",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val target = 15
      val commands = Vector.fill(target)(Dummy(party).create.command)
      for {
        request <- ledger.submitAndWaitRequest(party, commands: _*)
        _ <- ledger.submitAndWait(request)
        acs <- ledger.activeContracts(party)
      } yield {
        assert(
          acs.size == target,
          s"Expected $target contracts to be created, got ${acs.size} instead",
        )
      }
  }

  test(
    "CSCallablePayout",
    "Run CallablePayout and return the right events",
    allocate(TwoParties, SingleParty),
  ) {
    case Participants(Participant(alpha, giver, newReceiver), Participant(beta, receiver)) =>
      for {
        callablePayout <- alpha.create(giver, CallablePayout(giver, receiver))
        tree <- beta.exercise(receiver, callablePayout.exerciseTransfer(_, newReceiver))
      } yield {
        val created = assertSingleton("There should only be one creation", createdEvents(tree))
        assertEquals(
          "The created event should be the expected one",
          created.getCreateArguments.fields,
          encode(CallablePayout(giver, newReceiver)).getRecord.fields,
        )
      }
  }

  test(
    "CSReadyForExercise",
    "It should be possible to exercise a choice on a created contract",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        factory <- ledger.create(party, DummyFactory(party))
        tree <- ledger.exercise(party, factory.exerciseDummyFactoryCall)
      } yield {
        val exercise = assertSingleton("There should only be one exercise", exercisedEvents(tree))
        assert(exercise.contractId == factory.unwrap, "Contract identifier mismatch")
        assert(exercise.consuming, "The choice should have been consuming")
        val _ = assertLength("Two creations should have occurred", 2, createdEvents(tree))
      }
  }

  test("CSBadNumericValues", "Reject unrepresentable numeric values", allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      // Code generation catches bad decimals early so we have to do some work to create (possibly) invalid requests
      def rounding(numeric: String): Command =
        DecimalRounding(party, BigDecimal("0")).create.command.update(
          _.create.createArguments
            .fields(1) := RecordField(value = Some(Value(Value.Sum.Numeric(numeric)))),
        )
      val wouldLosePrecision = "0.00000000005"
      val positiveOutOfBounds = "10000000000000000000000000000.0000000000"
      val negativeOutOfBounds = "-10000000000000000000000000000.0000000000"
      for {
        r1 <- ledger.submitAndWaitRequest(party, rounding(wouldLosePrecision))
        e1 <- ledger.submitAndWait(r1).failed
        r2 <- ledger.submitAndWaitRequest(party, rounding(positiveOutOfBounds))
        e2 <- ledger.submitAndWait(r2).failed
        r3 <- ledger.submitAndWaitRequest(party, rounding(negativeOutOfBounds))
        e3 <- ledger.submitAndWait(r3).failed
      } yield {
        assertGrpcError(e1, Status.Code.INVALID_ARGUMENT, "Cannot represent")
        assertGrpcError(e2, Status.Code.INVALID_ARGUMENT, "Out-of-bounds (Numeric 10)")
        assertGrpcError(e3, Status.Code.INVALID_ARGUMENT, "Out-of-bounds (Numeric 10)")
      }
  }

  test("CSCreateAndExercise", "Implement create-and-exercise correctly", allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      val createAndExercise = Dummy(party).createAnd.exerciseDummyChoice1(party).command
      for {
        request <- ledger.submitAndWaitRequest(party, createAndExercise)
        _ <- ledger.submitAndWait(request)
        transactions <- ledger.flatTransactions(party)
        trees <- ledger.transactionTrees(party)
      } yield {
        assert(
          transactions.flatMap(_.events).isEmpty,
          "A create-and-exercise flat transaction should show no event",
        )
        assertEquals(
          "Unexpected template identifier in create event",
          trees.flatMap(createdEvents).map(_.getTemplateId),
          Vector(Dummy.id.unwrap),
        )
        val contractId = trees.flatMap(createdEvents).head.contractId
        assertEquals(
          "Unexpected exercise event triple (choice, contractId, consuming)",
          trees.flatMap(exercisedEvents).map(e => (e.choice, e.contractId, e.consuming)),
          Vector(("DummyChoice1", contractId, true)),
        )
      }
  }

  test(
    "CSBadCreateAndExercise",
    "Fail create-and-exercise on bad create arguments",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val createAndExercise = Dummy(party).createAnd
        .exerciseDummyChoice1(party)
        .command
        .update(_.createAndExercise.createArguments := Record())
      for {
        request <- ledger.submitAndWaitRequest(party, createAndExercise)
        failure <- ledger.submitAndWait(request).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Expecting 1 field for record")
      }
  }

  test(
    "CSCreateAndBadExerciseArguments",
    "Fail create-and-exercise on bad choice arguments",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val createAndExercise = Dummy(party).createAnd
        .exerciseDummyChoice1(party)
        .command
        .update(_.createAndExercise.choiceArgument := Value(Value.Sum.Bool(false)))
      for {
        request <- ledger.submitAndWaitRequest(party, createAndExercise)
        failure <- ledger.submitAndWait(request).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "mismatching type")
      }
  }

  test(
    "CSCreateAndBadExerciseChoice",
    "Fail create-and-exercise on invalid choice",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val missingChoice = "DoesNotExist"
      val createAndExercise = Dummy(party).createAnd
        .exerciseDummyChoice1(party)
        .command
        .update(_.createAndExercise.choice := missingChoice)
      for {
        request <- ledger.submitAndWaitRequest(party, createAndExercise)
        failure <- ledger.submitAndWait(request).failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          s"Couldn't find requested choice $missingChoice",
        )
      }
  }

}
