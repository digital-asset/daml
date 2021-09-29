// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.util.regex.Pattern

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Value.encode
import com.daml.ledger.test.model.Test.CallablePayout._
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.ledger.test.model.Test.DummyFactory._
import com.daml.ledger.test.model.Test.WithObservers._
import com.daml.ledger.test.model.Test._
import io.grpc.Status
import scalaz.syntax.tag._

final class CommandServiceIT extends LedgerTestSuite {
  test(
    "CSsubmitAndWaitBasic",
    "SubmitAndWait creates a contract of the expected template",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
      _ <- ledger.submitAndWait(request)
      active <- ledger.activeContracts(party)
    } yield {
      assert(active.size == 1)
      val dummyTemplateId = active.flatMap(_.templateId.toList).head
      assert(dummyTemplateId == Dummy.id.unwrap)
    }
  })

  test(
    "CSsubmitAndWaitForTransactionIdBasic",
    "SubmitAndWaitForTransactionId returns a valid transaction identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
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
  })

  test(
    "CSsubmitAndWaitForTransactionBasic",
    "SubmitAndWaitForTransaction returns a transaction",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
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
  })

  test(
    "CSsubmitAndWaitForTransactionTreeBasic",
    "SubmitAndWaitForTransactionTree returns a transaction tree",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
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
  })

  test(
    "CSduplicateSubmitAndWaitBasic",
    "SubmitAndWait should fail on duplicate requests",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
      _ <- ledger.submitAndWait(request)
      failure <- ledger.submitAndWait(request).mustFail("submitting a duplicate request")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.ALREADY_EXISTS,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSduplicateSubmitAndWaitForTransactionId",
    "SubmitAndWaitForTransactionId should fail on duplicate requests",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
      _ <- ledger.submitAndWaitForTransactionId(request)
      failure <- ledger
        .submitAndWaitForTransactionId(request)
        .mustFail("submitting a duplicate request")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.ALREADY_EXISTS,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSduplicateSubmitAndWaitForTransactionData",
    "SubmitAndWaitForTransaction should fail on duplicate requests",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
      _ <- ledger.submitAndWaitForTransaction(request)
      failure <- ledger
        .submitAndWaitForTransaction(request)
        .mustFail("submitting a duplicate request")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.ALREADY_EXISTS,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSduplicateSubmitAndWaitForTransactionTree",
    "SubmitAndWaitForTransactionTree should fail on duplicate requests",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
      _ <- ledger.submitAndWaitForTransactionTree(request)
      failure <- ledger
        .submitAndWaitForTransactionTree(request)
        .mustFail("submitting a duplicate request")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.ALREADY_EXISTS,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSsubmitAndWaitForTransactionIdInvalidLedgerId",
    "SubmitAndWaitForTransactionId should fail for invalid ledger IDs",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "CSsubmitAndWaitForTransactionIdInvalidLedgerId"
    val request = ledger
      .submitAndWaitRequest(party, Dummy(party).create.command)
      .update(_.commands.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .submitAndWaitForTransactionId(request)
        .mustFail("submitting a request with an invalid ledger ID")
    } yield assertGrpcError(
      failure,
      Status.Code.NOT_FOUND,
      Some(s"Ledger ID '$invalidLedgerId' not found."),
      checkDefiniteAnswerMetadata = true,
    )
  })

  test(
    "CSsubmitAndWaitForTransactionInvalidLedgerId",
    "SubmitAndWaitForTransaction should fail for invalid ledger IDs",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "CSsubmitAndWaitForTransactionInvalidLedgerId"
    val request = ledger
      .submitAndWaitRequest(party, Dummy(party).create.command)
      .update(_.commands.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .submitAndWaitForTransaction(request)
        .mustFail("submitting a request with an invalid ledger ID")
    } yield assertGrpcError(
      failure,
      Status.Code.NOT_FOUND,
      Some(s"Ledger ID '$invalidLedgerId' not found."),
      checkDefiniteAnswerMetadata = true,
    )
  })

  test(
    "CSsubmitAndWaitForTransactionTreeInvalidLedgerId",
    "SubmitAndWaitForTransactionTree should fail for invalid ledger ids",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "CSsubmitAndWaitForTransactionTreeInvalidLedgerId"
    val request = ledger
      .submitAndWaitRequest(party, Dummy(party).create.command)
      .update(_.commands.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .submitAndWaitForTransactionTree(request)
        .mustFail("submitting a request with an invalid ledger ID")
    } yield assertGrpcError(
      failure,
      Status.Code.NOT_FOUND,
      Some(s"Ledger ID '$invalidLedgerId' not found."),
      checkDefiniteAnswerMetadata = true,
    )
  })

  test(
    "CSRefuseBadParameter",
    "The submission of a creation that contains a bad parameter label should result in an INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val createWithBadArgument = Dummy(party).create.command
      .update(_.create.createArguments.fields.foreach(_.label := "INVALID_PARAM"))
    val badRequest = ledger.submitAndWaitRequest(party, createWithBadArgument)
    for {
      failure <- ledger
        .submitAndWait(badRequest)
        .mustFail("submitting a request with a bad parameter label")
    } yield {
      assertGrpcErrorRegex(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(Pattern.compile(s"Missing record (label|field)")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSReturnStackTrace",
    "A submission resulting in an interpretation error should return the stack trace",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      failure <- ledger
        .exercise(party, dummy.exerciseFailingClone)
        .mustFail("submitting a request with an interpretation error")
    } yield {
      // V1 message:
      // Invalid argument: Command interpretation error in LF-DAMLe: Interpretation error: Error: Unhandled exception: DA.Exception.AssertionFailed:AssertionFailed@3f4deaf1{ message = "Assertion failed" }. Details: Last location: [unknown source], partial transaction: root node NodeId(0): NodeExercises(ContractId(0093a9f35f1409d28c1645cb17a3fa3bffd592d028b2786a09a23d19d8f3a10311),c52d6e002359e5eb545555ea15400fdf951d9375e317e5815ba9151b3c615386:Test:Dummy,FailingClone,false,TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),ValueRecord(None,ImmArray()),TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),TreeSet(),ImmArray(NodeId(1),NodeId(3)),None,None,false,V14), node NodeId(1): NodeExercises(ContractId(0093a9f35f1409d28c1645cb17a3fa3bffd592d028b2786a09a23d19d8f3a10311),c52d6e002359e5eb545555ea15400fdf951d9375e317e5815ba9151b3c615386:Test:Dummy,Clone,false,TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),ValueRecord(None,ImmArray()),TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),TreeSet(),ImmArray(NodeId(2)),Some(ValueContractId(ContractId(00c3ca0f0c1701f24d8d4140ea7165ae2a5e32c56cbe34c65846b6fb4c495ed63c))),None,false,V14), node NodeId(2): NodeCreate(ContractId(00c3ca0f0c1701f24d8d4140ea7165ae2a5e32c56cbe34c65846b6fb4c495ed63c),c52d6e002359e5eb545555ea15400fdf951d9375e317e5815ba9151b3c615386:Test:Dummy,ValueRecord(None,ImmArray((None,ValueParty(CSReturnStackTrace-alpha-140f0256160b-party-0)))),'CSReturnStackTrace-alpha-140f0256160b-party-0' operates a dummy.,TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),None,V14), node NodeId(3): NodeExercises(ContractId(0093a9f35f1409d28c1645cb17a3fa3bffd592d028b2786a09a23d19d8f3a10311),c52d6e002359e5eb545555ea15400fdf951d9375e317e5815ba9151b3c615386:Test:Dummy,FailingChoice,true,TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),ValueRecord(None,ImmArray()),TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),TreeSet(CSReturnStackTrace-alpha-140f0256160b-party-0),TreeSet(),ImmArray(),None,None,false,V14).
      //
      // V2 message:
      // DAML_INTERPRETATION_ERROR(8,0): Interpretation error: Error: Unhandled exception: DA.Exception.AssertionFailed:AssertionFailed@3f4deaf1{ message = "Assertion failed" }
      assertGrpcErrorRegex(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(
          Pattern.compile(
            "Interpretation error: Error: (User abort: Assertion failed.|Unhandled exception: [0-9a-zA-Z\\.:]*@[0-9a-f]*\\{ message = \"Assertion failed\" \\}\\.) [Dd]etails(: |=)Last location: \\[[^\\]]*\\], partial transaction: root node"
          )
        ),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSDiscloseCreateToObservers",
    "Disclose create to observers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
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
  })

  test(
    "CSDiscloseExerciseToObservers",
    "Disclose exercise to observers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
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
  })

  test(
    "CSHugeCommandSubmission",
    "The server should accept a submission with 15 commands",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val target = 15
    val commands = Vector.fill(target)(Dummy(party).create.command)
    val request = ledger.submitAndWaitRequest(party, commands: _*)
    for {
      _ <- ledger.submitAndWait(request)
      acs <- ledger.activeContracts(party)
    } yield {
      assert(
        acs.size == target,
        s"Expected $target contracts to be created, got ${acs.size} instead",
      )
    }
  })

  test(
    "CSCallablePayout",
    "Run CallablePayout and return the right events",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, giver, newReceiver), Participant(beta, receiver)) =>
      for {
        callablePayout <- alpha.create(giver, CallablePayout(giver, receiver))
        _ <- synchronize(alpha, beta)
        tree <- beta.exercise(receiver, callablePayout.exerciseTransfer(_, newReceiver))
      } yield {
        val created = assertSingleton("There should only be one creation", createdEvents(tree))
        assertEquals(
          "The created event should be the expected one",
          created.getCreateArguments.fields,
          encode(CallablePayout(giver, newReceiver)).getRecord.fields,
        )
      }
  })

  test(
    "CSReadyForExercise",
    "It should be possible to exercise a choice on a created contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      factory <- ledger.create(party, DummyFactory(party))
      tree <- ledger.exercise(party, factory.exerciseDummyFactoryCall)
    } yield {
      val exercise = assertSingleton("There should only be one exercise", exercisedEvents(tree))
      assert(exercise.contractId == factory.unwrap, "Contract identifier mismatch")
      assert(exercise.consuming, "The choice should have been consuming")
      val _ = assertLength("Two creations should have occurred", 2, createdEvents(tree))
    }
  })

  test("CSBadNumericValues", "Reject unrepresentable numeric values", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, party)) =>
      // Code generation catches bad decimals early so we have to do some work to create (possibly) invalid requests
      def rounding(numeric: String): Command =
        DecimalRounding(party, BigDecimal("0")).create.command.update(
          _.create.createArguments
            .fields(1) := RecordField(value = Some(Value(Value.Sum.Numeric(numeric))))
        )
      val wouldLosePrecision = "0.00000000005"
      val positiveOutOfBounds = "10000000000000000000000000000.0000000000"
      val negativeOutOfBounds = "-10000000000000000000000000000.0000000000"
      for {
        e1 <- ledger
          .submitAndWait(ledger.submitAndWaitRequest(party, rounding(wouldLosePrecision)))
          .mustFail("submitting a request which would lose precision")
        e2 <- ledger
          .submitAndWait(ledger.submitAndWaitRequest(party, rounding(positiveOutOfBounds)))
          .mustFail("submitting a request with a positive number out of bounds")
        e3 <- ledger
          .submitAndWait(ledger.submitAndWaitRequest(party, rounding(negativeOutOfBounds)))
          .mustFail("submitting a request with a negative number out of bounds")
      } yield {
        assertGrpcError(
          e1,
          Status.Code.INVALID_ARGUMENT,
          Some("Cannot represent"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          e2,
          Status.Code.INVALID_ARGUMENT,
          Some("Out-of-bounds (Numeric 10)"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          e3,
          Status.Code.INVALID_ARGUMENT,
          Some("Out-of-bounds (Numeric 10)"),
          checkDefiniteAnswerMetadata = true,
        )
      }
    }
  )

  test("CSCreateAndExercise", "Implement create-and-exercise correctly", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, party)) =>
      val createAndExercise = Dummy(party).createAnd.exerciseDummyChoice1(party).command
      val request = ledger.submitAndWaitRequest(party, createAndExercise)
      for {
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
  )

  test(
    "CSBadCreateAndExercise",
    "Fail create-and-exercise on bad create arguments",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val createAndExercise = Dummy(party).createAnd
      .exerciseDummyChoice1(party)
      .command
      .update(_.createAndExercise.createArguments := Record())
    val request = ledger.submitAndWaitRequest(party, createAndExercise)
    for {
      failure <- ledger
        .submitAndWait(request)
        .mustFail("submitting a request with bad create arguments")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some("Expecting 1 field for record"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSCreateAndBadExerciseArguments",
    "Fail create-and-exercise on bad choice arguments",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val createAndExercise = Dummy(party).createAnd
      .exerciseDummyChoice1(party)
      .command
      .update(_.createAndExercise.choiceArgument := Value(Value.Sum.Bool(false)))
    val request = ledger.submitAndWaitRequest(party, createAndExercise)
    for {
      failure <- ledger
        .submitAndWait(request)
        .mustFail("submitting a request with bad choice arguments")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some("mismatching type"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSCreateAndBadExerciseChoice",
    "Fail create-and-exercise on invalid choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val missingChoice = "DoesNotExist"
    val createAndExercise = Dummy(party).createAnd
      .exerciseDummyChoice1(party)
      .command
      .update(_.createAndExercise.choice := missingChoice)
    val request = ledger.submitAndWaitRequest(party, createAndExercise)
    for {
      failure <- ledger
        .submitAndWait(request)
        .mustFail("submitting a request with an invalid choice")
    } yield {
      assertGrpcErrorRegex(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(
          Pattern.compile(
            "(unknown|Couldn't find requested) choice " + missingChoice
          )
        ),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

}
