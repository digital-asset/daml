// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy
import com.daml.lf.data.Ref
import com.google.protobuf.duration.Duration

final class CommandSubmissionCompletionDeduplicationIT extends LedgerTestSuite {

  test(
    "CSCDSuccessIncludeApplicationId",
    "The application ID is present in successful completions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, successfulCommand(party))
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
    "CSCDSuccessIncludeRequestedSubmissionId",
    "The requested submission ID is present in the request's successful completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, successfulCommand(party))
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
    "CSCDSuccessIncludeASubmissionIdWhenNotRequested",
    "A completion includes a submission ID when one is missing in the successful request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, successfulCommand(party))
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
    "CSCDSuccessIncludeRequestedDeduplicationOffset",
    "The requested deduplication offset is present in the associated successful completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, successfulCommand(party))
    val submittedDeduplication =
      DeduplicationPeriod.DeduplicationOffset("offset")
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
    "CSCDSuccessIncludeRequestedDeduplicationTime",
    "The requested deduplication time is present in the associated successful completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, successfulCommand(party))
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
    "CSCDSuccessIncludeNoDeduplicationWhenNotRequested",
    "A successful completion doesn't include a deduplication when one is missing in the request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, successfulCommand(party))
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

  private def successfulCommand(party: Primitive.Party) = Dummy(party).create.command
  private def failingCommand(party: Primitive.Party) = Dummy(party).createAnd.exerciseFailingChoice(party).command
}
