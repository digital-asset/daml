// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party, TestConstraints}
import com.daml.ledger.api.testtool.suites.v2_1.CommandServiceIT.{
  createEventToDisclosedContract,
  formatByPartyAndTemplate,
}
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.GetUpdatesRequest
import com.daml.ledger.api.v2.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.Command
import com.daml.ledger.test.java.model.test.*
import com.daml.ledger.test.java.semantic.divulgencetests.DummyFlexibleController
import com.digitalasset.base.error.{ErrorCategory, ErrorCode}
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.error.groups.{
  CommandExecutionErrors,
  ConsistencyErrors,
  RequestValidationErrors,
}
import org.scalatest.Inside.inside

import java.math.BigDecimal
import java.util.List as JList
import java.util.regex.Pattern
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

import CompanionImplicits.*

final class CommandServiceIT extends LedgerTestSuite with CommandSubmissionTestUtils {
  test(
    "CSsubmitAndWaitBasic",
    "CSsubmitAndWaitBasic returns a valid transaction identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitAndWaitRequest(party, new Dummy(party).create.commands)
    for {
      response <- ledger.submitAndWait(request)
      updateId = response.updateId
      retrievedTransaction <- ledger.transactionById(updateId, Seq(party))
      transactions <- ledger.transactions(AcsDelta, party)
    } yield {

      assert(updateId.nonEmpty, "The transaction identifier was empty but shouldn't.")
      assert(
        transactions.sizeIs == 1,
        s"$party should see only one transaction but sees ${transactions.size}",
      )

      val events = transactions.headOption.value.events

      assert(events.sizeIs == 1, s"$party should see only one event but sees ${events.size}")
      assert(
        events.headOption.value.event.isCreated,
        s"$party should see only one create but sees ${events.headOption.value.event}",
      )
      val created = transactions.headOption.value.events.headOption.value.getCreated

      assert(
        transactions.headOption.value.externalTransactionHash.isEmpty,
        "Expected empty external transaction hash for a local party transaction",
      )

      assert(
        retrievedTransaction.updateId == updateId,
        s"$party should see the transaction for the created contract $updateId but sees ${retrievedTransaction.updateId}",
      )
      assert(
        retrievedTransaction.events.sizeIs == 1,
        s"The retrieved transaction should contain a single event but contains ${retrievedTransaction.events.size}",
      )
      val retrievedEvent = retrievedTransaction.events.headOption.value

      assert(
        retrievedEvent.event.isCreated,
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
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request =
      ledger.submitAndWaitForTransactionRequest(
        party = party,
        commands = new Dummy(party).create.commands,
        transactionShape = AcsDelta,
      )
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(request)
    } yield {
      assertOnTransactionResponse(transactionResponse.getTransaction)
    }
  })

  test(
    "CSsubmitAndWaitForTransactionLedgerEffectsBasic",
    "SubmitAndWaitForTransaction with LedgerEffects returns a transaction with LedgerEffects",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitAndWaitForTransactionRequest(
      party,
      new Dummy(party).createAnd().exerciseDummyChoice1().commands,
      LedgerEffects,
    )
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(request)
    } yield {
      val transaction = transactionResponse.getTransaction
      assert(
        transaction.updateId.nonEmpty,
        "The transaction identifier was empty but shouldn't.",
      )
      assert(
        transaction.events.sizeIs == 2,
        s"The returned transaction should contain 2 events, but contained ${transaction.events.size}",
      )
      val event1 = transaction.events.headOption.value
      assert(
        event1.event.isCreated,
        s"The returned transaction should contain a created event, but was ${event1.event}",
      )
      val event2 = transaction.events.lastOption.value
      assert(
        event2.event.isExercised,
        s"The returned transaction should contain an exercised event, but was ${event2.event}",
      )
      assert(
        event1.getCreated.getTemplateId == Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
        s"The template ID of the created event should be ${Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toV1}, but was ${event1.getCreated.getTemplateId}",
      )
    }
  })

  test(
    "CSduplicateSubmitAndWaitBasic",
    "SubmitAndWait should fail on duplicate requests",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitAndWaitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submitAndWait(request)
      failure <- ledger
        .submitRequestAndTolerateGrpcError(
          ConsistencyErrors.SubmissionAlreadyInFlight,
          _.submitAndWait(request),
        )
        .mustFail("submitting a duplicate request")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.DuplicateCommand,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSSubmitAndWaitForTransactionFilterByParty",
    "SubmitAndWaitForTransaction returns a filtered transaction (by party)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitAndWaitForTransactionRequest(
      party = party,
      commands = (new Dummy(party).create.commands.asScala
        ++ new DummyFactory(party).create.commands.asScala).asJava,
      filterParties = Some(Seq(party)),
      transactionShape = AcsDelta,
    )
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(request)
      transaction = assertDefined(
        transactionResponse.transaction,
        "The transaction should be defined",
      )
    } yield {
      assertLength("Two create events should have been into the transaction", 2, transaction.events)
    }
  })

  test(
    "CSSubmitAndWaitForTransactionFilterByWrongParty",
    "SubmitAndWaitForTransaction returns a transaction with empty events when filtered by wrong party",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, party2))) =>
    def request = ledger.submitAndWaitForTransactionRequest(
      party = party,
      commands = (new Dummy(party).create.commands.asScala
        ++ new DummyFactory(party).create.commands.asScala).asJava,
      filterParties = Some(Seq(party2)),
      transactionShape = AcsDelta,
    )
    for {
      transactionResponseAcsDelta <- ledger.submitAndWaitForTransaction(request)
      transactionResponseLedgerEffects <- ledger.submitAndWaitForTransaction(
        request.update(_.transactionFormat.transactionShape := TRANSACTION_SHAPE_LEDGER_EFFECTS)
      )
    } yield {
      assertLength(
        "No events should have been into the transaction",
        0,
        transactionResponseAcsDelta.transaction.toList.flatMap(_.events),
      )
      assertLength(
        "No events should have been into the transaction",
        0,
        transactionResponseLedgerEffects.transaction.toList.flatMap(_.events),
      )
    }
  })

  test(
    "CSSubmitAndWaitForTransactionFilterAnyParty",
    "SubmitAndWaitForTransaction returns a filtered transaction (any party)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitAndWaitForTransactionRequest(
      party = party,
      commands = (new Dummy(party).create.commands.asScala
        ++ new DummyFactory(party).create.commands.asScala).asJava,
      filterParties = None,
      transactionShape = AcsDelta,
    )
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(request)
      transaction = assertDefined(
        transactionResponse.transaction,
        "The transaction should be defined",
      )
    } yield {
      assertLength("Two create events should have been into the transaction", 2, transaction.events)
    }
  })

  test(
    "CSSubmitAndWaitForTransactionFilterTemplate",
    "SubmitAndWaitForTransaction returns a filtered transaction (any party)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request = ledger.submitAndWaitForTransactionRequest(
      party = party,
      commands = (new DummyFactory(party).create.commands.asScala
        ++ new Dummy(party).create.commands.asScala).asJava,
      filterParties = None,
      transactionShape = AcsDelta,
      templateIds = Seq(Dummy.TEMPLATE_ID),
    )
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(request)
      transaction = assertDefined(
        transactionResponse.transaction,
        "The transaction should be defined",
      )
      event = assertSingleton(
        "One create event should have been into the transaction",
        transaction.events,
      )
    } yield {
      assertOnTransactionResponse(transactionResponse.getTransaction)
    }
  })

  test(
    "CSDuplicateSubmitAndWaitForTransactionData",
    "SubmitAndWaitForTransaction should fail on duplicate requests",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val request =
      ledger.submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands, AcsDelta)
    for {
      _ <- ledger.submitAndWaitForTransaction(request)
      failure <- ledger
        .submitRequestAndTolerateGrpcError(
          ConsistencyErrors.SubmissionAlreadyInFlight,
          _.submitAndWaitForTransaction(request),
        )
        .mustFail("submitting a duplicate request")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.DuplicateCommand,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSSubmitAndWaitForTransactionExplicitDisclosure",
    "SubmitAndWaitForTransaction returns an empty transaction when an explicitly disclosed contract is exercised",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner, stranger))) =>
    testExplicitDisclosure(
      ledger = ledger,
      owner = owner,
      stranger = stranger,
      submitAndWaitForTransactionRequest = (disclosedContract, contractId) =>
        ledger
          .submitAndWaitForTransactionRequest(
            party = stranger,
            commands = contractId.exerciseFlexibleConsume(stranger).commands,
            transactionShape = AcsDelta,
            filterParties = Some(Seq(stranger)),
          )
          .update(_.commands.disclosedContracts := scala.Seq(disclosedContract)),
      getTransactionLedgerEffects =
        (updateId, stranger) => ledger.transactionById(updateId, Seq(stranger), LedgerEffects),
    )
  })

  test(
    "CSsubmitAndWaitInvalidSynchronizerId",
    "SubmitAndWait should fail for invalid synchronizer ids",
    allocate(SingleParty),
  )(testInvalidSynchronizerIdSubmission(_.submitAndWait))

  test(
    "CSsubmitAndWaitForTransactionInvalidSynchronizerId",
    "SubmitAndWaitForTransaction should fail for invalid synchronizer ids",
    allocate(SingleParty),
  )(testInvalidSynchronizerIdSubmissionForTransaction(_.submitAndWaitForTransaction))

  test(
    "CSsubmitAndWaitPrescribedSynchronizerId",
    "SubmitAndWait should use the prescribed synchronizer id when routing the submission",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
  )({ implicit ec: ExecutionContext =>
    { case Participants(Participant(ledger, Seq(party, _*))) =>
      val (synchronizer1, synchronizer2) = inside(party.initialSynchronizers) {
        case Seq(synchronizer1, synchronizer2, _*) =>
          synchronizer1 -> synchronizer2
      }
      val requestForSynchronizer1 = ledger
        .submitAndWaitRequest(party, new Dummy(party).create.commands)
        .update(_.commands.synchronizerId := synchronizer1)
      for {
        actualSynchronizerId1 <- ledger
          .submitAndWait(requestForSynchronizer1)
          .map(_.updateId)
          .flatMap(updateId => ledger.transactionById(updateId, Seq(party), AcsDelta))
          .map(_.synchronizerId)
        requestForSynchronizer2 = ledger
          .submitAndWaitRequest(party, new Dummy(party).create.commands)
          .update(_.commands.synchronizerId := synchronizer2)
        actualSynchronizerId2 <- ledger
          .submitAndWait(requestForSynchronizer2)
          .map(_.updateId)
          .flatMap(updateId => ledger.transactionById(updateId, Seq(party), AcsDelta))
          .map(_.synchronizerId)
      } yield {
        assertEquals(actualSynchronizerId1, synchronizer1)
        assertEquals(actualSynchronizerId2, synchronizer2)
      }
    }
  })

  test(
    "CSsubmitAndWaitForTransactionPrescribedSynchronizerId",
    "SubmitAndWaitForTransaction should use the prescribed synchronizer id when routing the submission",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
  ) { implicit ec =>
    testValidSynchronizerIdSubmission(participant =>
      request =>
        participant
          .submitAndWaitForTransaction(request)
          .map(_.getTransaction.synchronizerId)
    )(ec)
  }

  test(
    "CSRefuseBadParameter",
    "The submission of a creation that contains a bad parameter label should result in an INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val createWithBadArgument =
      updateCommands(
        new Dummy(party).create.commands,
        _.update(_.create.createArguments.fields.foreach(_.label := "INVALID_PARAM")),
      )
    val badRequest = ledger.submitAndWaitRequest(party, createWithBadArgument)
    for {
      failure <- ledger
        .submitAndWait(badRequest)
        .mustFail("submitting a request with a bad parameter label")
    } yield {
      assertGrpcErrorRegex(
        failure,
        CommandExecutionErrors.Preprocessing.PreprocessingFailed,
        // both alternatives below are possible depends which problem is found first
        Some(Pattern.compile(s"Found non-optional extra field|Missing non-optional field")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  // TODO(#16361) fix this test: This test is not asserting that an interpretation error is returning a stack trace.
  //                     Furthermore, stack traces are not returned as of 1.18.
  //                     Instead more detailed error messages with the failed transaction are provided.
  test(
    "CSReturnStackTrace",
    "A submission resulting in an interpretation error should return the stack trace",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy: Dummy.ContractId <- ledger.create(party, new Dummy(party))
      failure <- ledger
        .exercise(party, dummy.exerciseFailingClone())
        .mustFail("submitting a request with an interpretation error")
    } yield {
      val trace =
        """    in choice [0-9a-f]{8}:Test:Dummy:FailingChoice on contract 00[0-9a-f]{8} \(#3\)
          |    in choice [0-9a-f]{8}:Test:Dummy:FailingClone on contract 00[0-9a-f]{8} \(#0\)
          |    in exercise command [0-9a-f]{8}:Test:Dummy:FailingClone on contract 00[0-9a-f]{8}""".stripMargin
      assertGrpcError(
        failure,
        new ErrorCode(
          CommandExecutionErrors.Interpreter.FailureStatus.id,
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        )(
          CommandExecutionErrors.Interpreter.FailureStatus.parent
        ) {},
        Some("Assertion failed"),
        checkDefiniteAnswerMetadata = true,
        additionalErrorAssertions = throwable =>
          assertMatches(
            "exercise_trace",
            extractErrorInfoMetadataValue(throwable, "exercise_trace"),
            Pattern.compile(trace),
          ),
      )
    }
  })

  test(
    "CSDiscloseCreateToObservers",
    "Disclose create to observers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(alpha, Seq(giver, observer1)),
          Participant(beta, Seq(observer2)),
        ) =>
      val template = new WithObservers(giver, List(observer1, observer2).map(_.getValue).asJava)
      for {
        _ <- alpha.create(giver, template)
        _ <- p.synchronize
        observer1View <- alpha.transactions(LedgerEffects, observer1)
        observer2View <- beta.transactions(LedgerEffects, observer2)
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
          Value.fromJavaProto(template.toValue.toProto).getRecord.fields,
        )
      }
  })

  test(
    "CSDiscloseExerciseToObservers",
    "Disclose exercise to observers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(alpha, Seq(giver, observer1)),
          Participant(beta, Seq(observer2)),
        ) =>
      val template = new WithObservers(giver, List(observer1, observer2).map(_.getValue).asJava)
      for {
        withObservers: WithObservers.ContractId <- alpha.create(giver, template)
        _ <- alpha.exercise(giver, withObservers.exercisePing())
        _ <- p.synchronize
        observer1View <- alpha.transactions(LedgerEffects, observer1)
        observer2View <- beta.transactions(LedgerEffects, observer2)
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
          observer1Exercise.contractId == withObservers.contractId,
          "The observers shouls see the exercised contract",
        )
      }
  })

  test(
    "CSHugeCommandSubmission",
    "The server should accept a submission with 15 commands",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val target = 15
    val commands = List.fill(target)(new Dummy(party).create.commands.asScala).flatten
    val request = ledger.submitAndWaitRequest(party, commands.asJava)
    for {
      _ <- ledger.submitAndWait(request)
      acs <- ledger.activeContracts(Some(Seq(party)))
    } yield {
      assert(
        acs.sizeIs == target,
        s"Expected $target contracts to be created, got ${acs.size} instead",
      )
    }
  })

  test(
    "CSCallablePayout",
    "Run CallablePayout and return the right events",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(alpha, Seq(giver, newReceiver)),
          Participant(beta, Seq(receiver)),
        ) =>
      for {
        callablePayout: CallablePayout.ContractId <- alpha.create(
          giver,
          new CallablePayout(giver, receiver),
        )
        _ <- p.synchronize
        tree <- beta.exercise(receiver, callablePayout.exerciseTransfer(newReceiver))
      } yield {
        val created = assertSingleton("There should only be one creation", createdEvents(tree))
        assertEquals(
          "The created event should be the expected one",
          created.getCreateArguments.fields,
          Value
            .fromJavaProto(new CallablePayout(giver, newReceiver).toValue.toProto)
            .getRecord
            .fields,
        )
      }
  })

  test(
    "CSReadyForExercise",
    "It should be possible to exercise a choice on a created contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      factory: DummyFactory.ContractId <- ledger.create(party, new DummyFactory(party))
      tree <- ledger.exercise(party, factory.exerciseDummyFactoryCall())
    } yield {
      val exercise = assertSingleton("There should only be one exercise", exercisedEvents(tree))
      assert(exercise.contractId == factory.contractId, "Contract identifier mismatch")
      assert(exercise.consuming, "The choice should have been consuming")
      val _ = assertLength("Two creations should have occurred", 2, createdEvents(tree))
    }
  })

  test("CSBadNumericValues", "Reject unrepresentable numeric values", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
      // Code generation catches bad decimals early so we have to do some work to create (possibly) invalid requests
      def rounding(numeric: String): JList[Command] =
        updateCommands(
          new DecimalRounding(party, BigDecimal.valueOf(0)).create.commands,
          _.update(
            _.create.createArguments
              .fields(1) := RecordField(value = Some(Value(Value.Sum.Numeric(numeric))))
          ),
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
          CommandExecutionErrors.Preprocessing.PreprocessingFailed,
          Some("Cannot represent"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          e2,
          CommandExecutionErrors.Preprocessing.PreprocessingFailed,
          Some("Out-of-bounds (Numeric 10)"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          e3,
          CommandExecutionErrors.Preprocessing.PreprocessingFailed,
          Some("Out-of-bounds (Numeric 10)"),
          checkDefiniteAnswerMetadata = true,
        )
      }
    }
  )

  test("CSCreateAndExercise", "Implement create-and-exercise correctly", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
      val createAndExercise = new Dummy(party).createAnd.exerciseDummyChoice1().commands
      val request = ledger.submitAndWaitRequest(party, createAndExercise)
      for {
        _ <- ledger.submitAndWait(request)
        transactions <- ledger.transactions(AcsDelta, party)
        transactionsLedgerEffects <- ledger.transactions(LedgerEffects, party)
      } yield {
        assert(
          transactions.flatMap(_.events).isEmpty,
          "A create-and-exercise acs delta transaction should show no event",
        )
        assertEquals(
          "Unexpected template identifier in create event",
          transactionsLedgerEffects.flatMap(createdEvents).map(_.getTemplateId),
          Vector(Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toV1),
        )
        val contractId =
          transactionsLedgerEffects.flatMap(createdEvents).headOption.value.contractId
        assertEquals(
          "Unexpected exercise event triple (choice, contractId, consuming)",
          transactionsLedgerEffects
            .flatMap(exercisedEvents)
            .map(e => (e.choice, e.contractId, e.consuming)),
          Vector(("DummyChoice1", contractId, true)),
        )
      }
    }
  )

  test(
    "CSBadCreateAndExercise",
    "Fail create-and-exercise on bad create arguments",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val createAndExercise =
      updateCommands(
        new Dummy(party).createAnd
          .exerciseDummyChoice1()
          .commands,
        _.update(_.createAndExercise.createArguments := Record()),
      )
    val request = ledger.submitAndWaitRequest(party, createAndExercise)
    for {
      failure <- ledger
        .submitAndWait(request)
        .mustFail("submitting a request with bad create arguments")
    } yield {
      assertGrpcError(
        failure,
        CommandExecutionErrors.Preprocessing.PreprocessingFailed,
        Some("Missing non-optional field"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSCreateAndBadExerciseArguments",
    "Fail create-and-exercise on bad choice arguments",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly("Problem creating faulty JSON from a faulty GRPC call"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val createAndExercise =
      updateCommands(
        new Dummy(party).createAnd
          .exerciseDummyChoice1()
          .commands,
        _.update(_.createAndExercise.choiceArgument := Value(Value.Sum.Bool(value = false))),
      )
    val request = ledger.submitAndWaitRequest(party, createAndExercise)
    for {
      failure <- ledger
        .submitAndWait(request)
        .mustFail("submitting a request with bad choice arguments")
    } yield {
      assertGrpcError(
        failure,
        CommandExecutionErrors.Preprocessing.PreprocessingFailed,
        Some("mismatching type"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSCreateAndBadExerciseChoice",
    "Fail create-and-exercise on invalid choice",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly("Problem creating faulty JSON from a faulty GRPC call"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val missingChoice = "DoesNotExist"
    val createAndExercise =
      updateCommands(
        new Dummy(party).createAnd
          .exerciseDummyChoice1()
          .commands,
        _.update(_.createAndExercise.choice := missingChoice),
      )
    val request = ledger.submitAndWaitRequest(party, createAndExercise)
    for {
      failure <- ledger
        .submitAndWait(request)
        .mustFail("submitting a request with an invalid choice")
    } yield {
      assertGrpcErrorRegex(
        failure,
        CommandExecutionErrors.Preprocessing.PreprocessingFailed,
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
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    def request = ledger.submitAndWaitRequest(party, new Dummy(party).create.commands)
    def requestForTransaction =
      ledger.submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands, AcsDelta)
    def requestForTransactionTree =
      ledger.submitAndWaitForTransactionRequest(
        party,
        new Dummy(party).create.commands,
        LedgerEffects,
      )
    for {
      updateIdResponse <- ledger.submitAndWait(request)
      retrievedTransaction <- ledger.transactionById(
        updateIdResponse.updateId,
        Seq(party),
        LedgerEffects,
      )
      transactionResponse <- ledger.submitAndWaitForTransaction(requestForTransaction)
      transactionTreeResponse <- ledger.submitAndWaitForTransaction(requestForTransactionTree)
    } yield {
      assert(
        updateIdResponse.completionOffset > 0 && updateIdResponse.completionOffset == retrievedTransaction.offset,
        "SubmitAndWait does not contain the expected completion offset",
      )
      assert(
        transactionResponse.getTransaction.offset > 0,
        "SubmitAndWaitForTransaction does not contain the expected completion offset",
      )
      assert(
        transactionTreeResponse.getTransaction.offset > 0,
        "SubmitAndWaitForTransactionTree does not contain the expected completion offset",
      )
    }
  })

  test(
    "CSSubmitAndWaitForTransactionNonConsumingChoice",
    "SubmitAndWaitForTransaction returns an empty transaction when command contains only a non-consuming choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      contractId: Dummy.ContractId <- ledger.create(owner, new Dummy(owner))
      response <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(
          owner,
          contractId.exerciseDummyNonConsuming().commands,
          AcsDelta,
        )
      )
      offset = response.transaction.value.offset
      transaction = assertDefined(
        response.transaction,
        "the transaction should have been returned",
      )
      _ <- fetchAndCompareTransactions(
        transaction,
        ledger.transactionByOffset(offset, Seq(owner), LedgerEffects),
      )
    } yield ()
  })

  private def testExplicitDisclosure(
      ledger: ParticipantTestContext,
      owner: Party,
      stranger: Party,
      submitAndWaitForTransactionRequest: (
          DisclosedContract,
          DummyFlexibleController.ContractId,
      ) => SubmitAndWaitForTransactionRequest,
      getTransactionLedgerEffects: (String, Party) => Future[Transaction],
  )(implicit ec: ExecutionContext): Future[Unit] = for {
    contractId: DummyFlexibleController.ContractId <- ledger.create(
      owner,
      new DummyFlexibleController(owner),
    )
    end <- ledger.currentEnd()
    witnessTxs <- ledger.transactions(
      new GetUpdatesRequest(
        beginExclusive = ledger.begin,
        endInclusive = Some(end),
        updateFormat = Some(formatByPartyAndTemplate(owner, DummyFlexibleController.TEMPLATE_ID)),
      )
    )
    tx = assertSingleton("Owners' transactions", witnessTxs)
    create = assertSingleton("The create", createdEvents(tx))
    disclosedContract = createEventToDisclosedContract(create)
    submitResponse <- ledger.submitAndWaitForTransaction(
      submitAndWaitForTransactionRequest(disclosedContract, contractId)
    )
    transaction = assertDefined(
      submitResponse.transaction,
      "the transaction should have been returned",
    )
    _ <- fetchAndCompareTransactions(
      transaction = transaction,
      getTransactionLedgerEffects = getTransactionLedgerEffects(transaction.updateId, stranger),
    )
  } yield ()

  private def fetchAndCompareTransactions(
      transaction: Transaction,
      getTransactionLedgerEffects: => Future[Transaction],
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      transactionLedgerEffects <- getTransactionLedgerEffects
    } yield assertEquals(
      "The transaction should contain the same details (except events) as the ledger effects transaction",
      transaction,
      Transaction(
        updateId = transactionLedgerEffects.updateId,
        commandId = transactionLedgerEffects.commandId,
        workflowId = transactionLedgerEffects.workflowId,
        effectiveAt = transactionLedgerEffects.effectiveAt,
        events = Seq.empty,
        offset = transactionLedgerEffects.offset,
        synchronizerId = transactionLedgerEffects.synchronizerId,
        traceContext = transactionLedgerEffects.traceContext,
        recordTime = transactionLedgerEffects.recordTime,
        externalTransactionHash = transactionLedgerEffects.externalTransactionHash,
      ),
    )

  private def testValidSynchronizerIdSubmission(
      submitAndWaitEndpoint: ParticipantTestContext => SubmitAndWaitForTransactionRequest => Future[
        String
      ]
  ): ExecutionContext => PartialFunction[Participants, Future[Unit]] = {
    implicit ec: ExecutionContext =>
      { case Participants(Participant(ledger, Seq(party, _*))) =>
        val (synchronizer1, synchronizer2) = inside(party.initialSynchronizers) {
          case Seq(synchronizer1, synchronizer2, _*) =>
            synchronizer1 -> synchronizer2
        }
        val requestForSynchronizer1 = ledger
          .submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands, AcsDelta)
          .update(_.commands.synchronizerId := synchronizer1)
        for {
          actualSynchronizerId1 <- submitAndWaitEndpoint(ledger)(requestForSynchronizer1)
          requestForSynchronizer2 = ledger
            .submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands)
            .update(_.commands.synchronizerId := synchronizer2)
          actualSynchronizerId2 <- submitAndWaitEndpoint(ledger)(requestForSynchronizer2)
        } yield {
          assertEquals(actualSynchronizerId1, synchronizer1)
          assertEquals(actualSynchronizerId2, synchronizer2)
        }
      }
  }

  private def testInvalidSynchronizerIdSubmission[R](
      submitAndWaitEndpoint: ParticipantTestContext => SubmitAndWaitRequest => Future[R]
  ): ExecutionContext => PartialFunction[Participants, Future[Unit]] = {
    implicit ec: ExecutionContext =>
      { case Participants(Participant(ledger, Seq(party, _*))) =>
        val invalidSynchronizerId = "invalidSynchronizerId"
        val request: SubmitAndWaitRequest = ledger
          .submitAndWaitRequest(party, new Dummy(party).create.commands)
          .update(_.commands.synchronizerId := invalidSynchronizerId)
        for {
          failure <- submitAndWaitEndpoint(ledger)(request).mustFail(
            "submitting a request with an invalid synchronizer id"
          )
        } yield assertGrpcError(
          failure,
          RequestValidationErrors.InvalidField,
          Some(
            s"Invalid field synchronizer_id: Invalid unique identifier `$invalidSynchronizerId` with missing namespace."
          ),
          checkDefiniteAnswerMetadata = true,
        )
      }
  }

  private def testInvalidSynchronizerIdSubmissionForTransaction[R](
      submitAndWaitEndpoint: ParticipantTestContext => SubmitAndWaitForTransactionRequest => Future[
        R
      ]
  ): ExecutionContext => PartialFunction[Participants, Future[Unit]] = {
    implicit ec: ExecutionContext =>
      { case Participants(Participant(ledger, Seq(party, _*))) =>
        val invalidSynchronizerId = "invalidSynchronizerId"
        val request: SubmitAndWaitForTransactionRequest = ledger
          .submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands, AcsDelta)
          .update(_.commands.synchronizerId := invalidSynchronizerId)
        for {
          failure <- submitAndWaitEndpoint(ledger)(request).mustFail(
            "submitting a request with an invalid synchronizer id"
          )
        } yield assertGrpcError(
          failure,
          RequestValidationErrors.InvalidField,
          Some(
            s"Invalid field synchronizer_id: Invalid unique identifier `$invalidSynchronizerId` with missing namespace."
          ),
          checkDefiniteAnswerMetadata = true,
        )
      }
  }

}

object CommandServiceIT {
  def createEventToDisclosedContract(ev: CreatedEvent): DisclosedContract =
    DisclosedContract(
      templateId = ev.templateId,
      contractId = ev.contractId,
      createdEventBlob = ev.createdEventBlob,
      synchronizerId = "",
    )

  def formatByPartyAndTemplate(
      owner: Party,
      templateId: javaapi.data.Identifier,
  ): UpdateFormat = {
    val templateIdScalaPB = Identifier.fromJavaProto(templateId.toProto)

    val eventFormat = EventFormat(
      filtersByParty = Map(
        owner.getValue -> new Filters(
          Seq(
            CumulativeFilter(
              IdentifierFilter.TemplateFilter(
                TemplateFilter(Some(templateIdScalaPB), includeCreatedEventBlob = true)
              )
            )
          )
        )
      ),
      filtersForAnyParty = None,
      verbose = false,
    )

    UpdateFormat(
      includeTransactions = Some(
        TransactionFormat(
          eventFormat = Some(eventFormat),
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )
      ),
      includeReassignments = None,
      includeTopologyEvents = None,
    )
  }
}
