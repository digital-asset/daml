// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.regex.Pattern

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, TimeoutException, WithTimeout}
import com.daml.ledger.test.java.model.test.Dummy

import scala.concurrent.duration.DurationInt

final class CommandSubmissionCompletionIT extends LedgerTestSuite {
  import CompanionImplicits._

  test(
    "CSCCompletions",
    "Read completions correctly with a correct application identifier and reading party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      completions <- ledger.firstCompletions(party)
    } yield {
      val commandId =
        assertSingleton("Expected only one completion", completions.map(_.commandId))
      assert(
        commandId == request.commands.get.commandId,
        "Wrong command identifier on completion",
      )
    }
  })

  test(
    "CSCNoCompletionsWithoutRightAppId",
    "Read no completions without the correct application identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      invalidRequest = ledger
        .completionStreamRequest()(party)
        .update(_.applicationId := "invalid-application-id")
      failure <- WithTimeout(5.seconds)(ledger.firstCompletions(invalidRequest))
        .mustFail("subscribing to completions with an invalid application ID")
    } yield {
      assert(failure == TimeoutException, "Timeout expected")
    }
  })

  test(
    "CSCAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing to completions past the ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      futureOffset <- ledger.offsetBeyondLedgerEnd()
      invalidRequest = ledger
        .completionStreamRequest()(party)
        .update(_.offset := futureOffset)
      failure <- ledger
        .firstCompletions(invalidRequest)
        .mustFail("subscribing to completions past the ledger end")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.OffsetAfterLedgerEnd,
        Some("is after ledger end"),
      )
    }
  })

  test(
    "CSCNoCompletionsWithoutRightParty",
    "Read no completions without the correct party",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, party, notTheSubmittingParty)) =>
    val request = ledger.submitRequest(party, new Dummy(party).create.commands)
    for {
      _ <- ledger.submit(request)
      failure <- WithTimeout(5.seconds)(ledger.firstCompletions(notTheSubmittingParty))
        .mustFail("subscribing to completions with the wrong party")
    } yield {
      assert(failure == TimeoutException, "Timeout expected")
    }
  })

  test(
    "CSCRefuseBadChoice",
    "The submission of an exercise of a choice that does not exist should yield INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val badChoice = "THIS_IS_NOT_A_VALID_CHOICE"
    for {
      dummy <- ledger.create(party, new Dummy(party))
      exercise = dummy.exerciseDummyChoice1().commands
      wrongExercise = updateCommands(exercise, _.update(_.exercise.choice := badChoice))
      wrongRequest = ledger.submitRequest(party, wrongExercise)
      failure <- ledger.submit(wrongRequest).mustFail("submitting an invalid choice")
    } yield {
      assertGrpcErrorRegex(
        failure,
        LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
        Some(
          Pattern.compile(
            "(unknown|Couldn't find requested) choice " + badChoice
          )
        ),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSCSubmitWithInvalidLedgerId",
    "Submit should fail for an invalid ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "CSsubmitAndWaitInvalidLedgerId"
    val request = ledger
      .submitRequest(party, new Dummy(party).create.commands)
      .update(_.commands.ledgerId := invalidLedgerId)
    for {
      failure <- ledger.submit(request).mustFail("submitting with an invalid ledger ID")
    } yield assertGrpcError(
      failure,
      LedgerApiErrors.RequestValidation.LedgerIdMismatch,
      Some(s"Ledger ID '$invalidLedgerId' not found."),
      checkDefiniteAnswerMetadata = true,
    )
  })

  test(
    "CSCDisallowEmptyTransactionsSubmission",
    "The submission of an empty command should be rejected with INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val emptyRequest = ledger.submitRequest(party)
    for {
      failure <- ledger.submit(emptyRequest).mustFail("submitting an empty command")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.MissingField,
        Some("commands"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "CSCHandleMultiPartySubscriptions",
    "Listening for completions should support multi-party subscriptions",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val aliceRequest = ledger.submitRequest(alice, new Dummy(alice).create.commands)
    val bobRequest = ledger.submitRequest(bob, new Dummy(bob).create.commands)
    val aliceCommandId = aliceRequest.getCommands.commandId
    val bobCommandId = bobRequest.getCommands.commandId
    for {
      _ <- ledger.submit(aliceRequest)
      _ <- ledger.submit(bobRequest)
      _ <- WithTimeout(5.seconds)(ledger.findCompletion(alice, bob)(_.commandId == aliceCommandId))
      _ <- WithTimeout(5.seconds)(ledger.findCompletion(alice, bob)(_.commandId == bobCommandId))
    } yield {
      // Nothing to do, if the two completions are found the test is passed
    }
  })
}
