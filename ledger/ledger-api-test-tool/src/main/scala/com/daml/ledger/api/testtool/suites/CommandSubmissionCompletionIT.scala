// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.testing.utils.MockMessages.offset
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.test.model.Test.Dummy
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.lf.data.Ref
import com.daml.platform.testing.{TimeoutException, WithTimeout}
import com.google.protobuf.duration.Duration
import io.grpc.Status

import java.util.regex.Pattern
import scala.concurrent.duration.DurationInt
import scala.util.Try

final class CommandSubmissionCompletionIT extends LedgerTestSuite {

  test(
    "CSCCompletions",
    "Read completions correctly with a correct application identifier and reading party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
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
    val request = ledger.submitRequest(party, Dummy(party).create.command)
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
    val request = ledger.submitRequest(party, Dummy(party).create.command)
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
      assertGrpcError(failure, Status.Code.OUT_OF_RANGE, "is after ledger end")
    }
  })

  test(
    "CSCNoCompletionsWithoutRightParty",
    "Read no completions without the correct party",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, party, notTheSubmittingParty)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
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
      dummy <- ledger.create(party, Dummy(party))
      exercise = dummy.exerciseDummyChoice1(party).command
      wrongExercise = exercise.update(_.exercise.choice := badChoice)
      wrongRequest = ledger.submitRequest(party, wrongExercise)
      failure <- ledger.submit(wrongRequest).mustFail("submitting an invalid choice")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(
          Pattern.compile(
            "(unknown|Couldn't find requested) choice " + badChoice
          )
        ),
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
      .submitRequest(party, Dummy(party).create.command)
      .update(_.commands.ledgerId := invalidLedgerId)
    for {
      failure <- ledger.submit(request).mustFail("submitting with an invalid ledger ID")
    } yield assertGrpcError(
      failure,
      Status.Code.NOT_FOUND,
      s"Ledger ID '$invalidLedgerId' not found.",
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
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Missing field: commands")
    }
  })

  test(
    "CSCHandleMultiPartySubscriptions",
    "Listening for completions should support multi-party subscriptions",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val aliceRequest = ledger.submitRequest(alice, Dummy(alice).create.command)
    val bobRequest = ledger.submitRequest(bob, Dummy(bob).create.command)
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

  test(
    "CSCIncludeApplicationId",
    "The application ID is present in completions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    for {
      _ <- ledger.submit(request)
      completions <- ledger.firstCompletions(party)
    } yield {
      val actualCompletionApplicationId = singleCompletion(completions).applicationId
      assert(
        Ref.ApplicationId.fromString(actualCompletionApplicationId).contains(ledger.applicationId),
        "Wrong application ID in completion, " +
          s"expected: ${ledger.applicationId}, actual: $actualCompletionApplicationId",
      )
    }
  })

  test(
    "CSCIncludeRequestedSubmissionId",
    "The requested submission ID is present in the request's completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    val requestedSubmissionId =
      Some(Ref.SubmissionId.assertFromString(SubmissionIdGenerator.Random.generate()))
    for {
      _ <- ledger.submit(update(request, optSubmissionId = requestedSubmissionId))
      completions <- ledger.firstCompletions(party)
    } yield {
      val actualCompletionSubmissionId = Option(singleCompletion(completions).submissionId)
      assert(
        actualCompletionSubmissionId == requestedSubmissionId,
        "Wrong submission ID in completion, " +
          s"expected: $requestedSubmissionId, actual: $actualCompletionSubmissionId",
      )
    }
  })

  test(
    "CSCIncludeASubmissionIdWhenNotRequested",
    "A completion includes a submission ID when one is missing in the request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    for {
      _ <- ledger.submit(request)
      completions <- ledger.firstCompletions(party)
    } yield {
      val actualCompletionSubmissionId = singleCompletion(completions).submissionId
      assert(
        Ref.SubmissionId.fromString(actualCompletionSubmissionId).isRight,
        "Missing or invalid submission ID in completion",
      )
    }
  })

  test(
    "CSCIncludeRequestedDeduplicationOffset",
    "The requested deduplication offset is present in the request's completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    val submittedDeduplication =
      DeduplicationPeriod.DeduplicationOffset(offset(0).toHexString)
    for {
      _ <- ledger.submit(update(request, optDeduplicationPeriod = Some(submittedDeduplication)))
      completions <- ledger.firstCompletions(party)
    } yield {
      val actualCompletionDeduplication = singleCompletion(completions).deduplicationPeriod
      assert(
        actualCompletionDeduplication.deduplicationOffset == submittedDeduplication.deduplicationOffset,
        "Wrong duplication offset in completion, " +
          s"expected: ${submittedDeduplication.deduplicationOffset}, " +
          s"actual: ${actualCompletionDeduplication.deduplicationOffset}",
      )
    }
  })

  test(
    "CSCIncludeRequestedDeduplicationTime",
    "The requested deduplication time is present in the request's completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    val submittedDeduplication =
      DeduplicationPeriod.DeduplicationTime(Duration(seconds = 100, nanos = 10))
    for {
      _ <- ledger.submit(update(request, optDeduplicationPeriod = Some(submittedDeduplication)))
      completions <- ledger.firstCompletions(party)
    } yield {
      val actualCompletionDeduplication = singleCompletion(completions).deduplicationPeriod
      assert(
        actualCompletionDeduplication.deduplicationTime == submittedDeduplication.deduplicationTime,
        "Wrong duplication time in completion, " +
          s"expected: ${submittedDeduplication.deduplicationTime}, " +
          s"actual: ${actualCompletionDeduplication.deduplicationTime}",
      )
    }
  })

  test(
    "CSCIncludeNoDeduplicationWhenNotRequested",
    "A completion doesn't include a deduplication when one is missing in the request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    for {
      _ <- ledger.submit(request)
      completions <- ledger.firstCompletions(party)
    } yield {
      val actualCompletionDeduplication = singleCompletion(completions).deduplicationPeriod
      assert(
        actualCompletionDeduplication.isEmpty,
        s"The deduplication $actualCompletionDeduplication " +
          "is present in the completion even though it was not requested",
      )
    }
  })

  private def update(
      submitRequest: SubmitRequest,
      optSubmissionId: Option[Ref.SubmissionId] = None,
      optDeduplicationPeriod: Option[DeduplicationPeriod] = None,
  ): SubmitRequest = {
    val optRequestUpdatedWithSubmission =
      optSubmissionId.map { submissionId =>
        submitRequest.copy(commands =
          submitRequest.commands.map(_.copy(submissionId = submissionId))
        )
      }

    val optRequestUpdatedWithDeduplicationPeriod =
      optDeduplicationPeriod.map { deduplicationPeriod =>
        optRequestUpdatedWithSubmission
          .getOrElse(submitRequest)
          .copy(commands =
            submitRequest.commands.map(_.copy(deduplicationPeriod = deduplicationPeriod))
          )
      }

    optRequestUpdatedWithDeduplicationPeriod.getOrElse(submitRequest)
  }

  private def singleCompletion(completions: Seq[Completion]): Completion =
    assertSingleton(
      "Expected exactly one completion",
      completions,
    )
}
