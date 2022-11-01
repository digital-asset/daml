// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.regex.Pattern
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.v1.command_service.SubmitAndWaitForTransactionResponse
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Value.encode
import com.daml.ledger.test.model.Test.CallablePayout._
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.ledger.test.model.Test.DummyFactory._
import com.daml.ledger.test.model.Test.WithObservers._
import com.daml.ledger.test.model.Test._
import com.daml.platform.error.definitions.LedgerApiErrors
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
      transactionIdResponse <- ledger.submitAndWaitForTransactionId(request)
      transactionId = transactionIdResponse.transactionId
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
      transactionResponse <- ledger.submitAndWaitForTransaction(request)
    } yield {
      assertOnTransactionResponse(transactionResponse)
    }
  })

  test(
    "CSsubmitAndWaitForTransactionEmptyLedgerId",
    "SubmitAndWaitForTransaction should accept requests with empty ledgerId",
    allocate(SingleParty),
    enabled = _.optionalLedgerId,
    disabledReason = "Optional ledger id must be enabled",
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger
      .submitAndWaitRequest(party, Dummy(party).create.command)
      .update(_.commands.ledgerId := "")
    for {
      transactionResponse <- ledger
        .submitAndWaitForTransaction(request)
    } yield {
      assertOnTransactionResponse(transactionResponse)
    }
  })

  private def assertOnTransactionResponse(
      transactionResponse: SubmitAndWaitForTransactionResponse
  ): Unit = {
    val transaction = transactionResponse.getTransaction
    assert(
      transaction.transactionId.nonEmpty,
      "The transaction identifier was empty but shouldn't.",
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

  test(
    "CSsubmitAndWaitForTransactionTreeBasic",
    "SubmitAndWaitForTransactionTree returns a transaction tree",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
      transactionTreeResponse <- ledger.submitAndWaitForTransactionTree(request)
    } yield {
      val transactionTree = transactionTreeResponse.getTransaction
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
        LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
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
        LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
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
        LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
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
        LedgerApiErrors.ConsistencyErrors.DuplicateCommand,
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
      LedgerApiErrors.RequestValidation.LedgerIdMismatch,
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
      LedgerApiErrors.RequestValidation.LedgerIdMismatch,
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
      LedgerApiErrors.RequestValidation.LedgerIdMismatch,
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
        LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
        Some(Pattern.compile(s"Missing record (label|field)")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  // TODO fix this test: This test is not asserting that an interpretation error is returning a stack trace.
  //                     Furthermore, stack traces are not returned as of 1.18.
  //                     Instead more detailed error messages with the failed transaction are provided.
  test(
    "CSReturnStackTrace",
    "A submission resulting in an interpretation error should return the stack trace",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      failure <- ledger
        .exercise(party, dummy.exerciseFailingClone())
        .mustFail("submitting a request with an interpretation error")
    } yield {
      assertGrpcErrorRegex(
        failure,
        LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError,
        Some(
          Pattern.compile(
            "Interpretation error: Error: (User abort: Assertion failed.?|Unhandled (Daml )?exception: [0-9a-zA-Z\\.:]*@[0-9a-f]*\\{ message = \"Assertion failed\" \\}\\. [Dd]etails(: |=)Last location: \\[[^\\]]*\\], partial transaction: root node)"
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
        _ <- alpha.exercise(giver, withObservers.exercisePing())
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
        tree <- beta.exercise(receiver, callablePayout.exerciseTransfer(newReceiver))
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
      tree <- ledger.exercise(party, factory.exerciseDummyFactoryCall())
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
          LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
          Some("Cannot represent"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          e2,
          LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
          Some("Out-of-bounds (Numeric 10)"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          e3,
          LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
          Some("Out-of-bounds (Numeric 10)"),
          checkDefiniteAnswerMetadata = true,
        )
      }
    }
  )

  test("CSCreateAndExercise", "Implement create-and-exercise correctly", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, party)) =>
      val createAndExercise = Dummy(party).createAnd.exerciseDummyChoice1().command
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
      .exerciseDummyChoice1()
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
        LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
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
      .exerciseDummyChoice1()
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
        LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
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
      .exerciseDummyChoice1()
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
        LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
        Some(
          Pattern.compile(
            "(unknown|Couldn't find requested) choice " + missingChoice
          )
        ),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSsubmitAndWaitCompletionOffset",
    "SubmitAndWait methods return the completion offset in the response",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    def request = ledger.submitAndWaitRequest(party, Dummy(party).create.command)
    for {
      transactionIdResponse <- ledger.submitAndWaitForTransactionId(request)
      retrievedTransaction <- ledger.transactionTreeById(transactionIdResponse.transactionId, party)
      transactionResponse <- ledger.submitAndWaitForTransaction(request)
      transactionTreeResponse <- ledger.submitAndWaitForTransactionTree(request)
    } yield {
      assert(
        transactionIdResponse.completionOffset.nonEmpty &&
          transactionIdResponse.completionOffset == retrievedTransaction.offset,
        "SubmitAndWaitForTransactionId does not contain the expected completion offset",
      )
      assert(
        transactionResponse.completionOffset.nonEmpty &&
          transactionResponse.completionOffset == transactionResponse.getTransaction.offset,
        "SubmitAndWaitForTransaction does not contain the expected completion offset",
      )
      assert(
        transactionTreeResponse.completionOffset.nonEmpty
          && transactionTreeResponse.completionOffset
          == transactionTreeResponse.getTransaction.offset,
        "SubmitAndWaitForTransactionTree does not contain the expected completion offset",
      )
    }
  })

}
