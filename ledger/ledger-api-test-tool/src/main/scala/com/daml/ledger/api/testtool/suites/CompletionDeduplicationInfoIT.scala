// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.suites.CompletionDeduplicationInfoIT._
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy
import com.daml.lf.data.Ref
import com.google.protobuf.duration.Duration

import scala.concurrent.{ExecutionContext, Future}

final class CompletionDeduplicationInfoIT(service: Service) extends LedgerTestSuite {

  override private[testtool] def name = service.productPrefix + super.name

  test(
    "CCDIIncludeApplicationId",
    "The application ID is present in completions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- submitSuccessfulAndFailingRequests(service, ledger, party)
      commandSubmissionCompletions <- ledger.firstCompletions(party)
    } yield {
      assertCompletionsCount(commandSubmissionCompletions, 2)
      commandSubmissionCompletions.foreach { completion =>
        val expectedApplicationId = ledger.applicationId
        val actualApplicationId = completion.applicationId
        assert(
          Ref.ApplicationId.fromString(actualApplicationId).contains(expectedApplicationId),
          "Wrong application ID in completion, " +
            s"expected: $expectedApplicationId, actual: $actualApplicationId",
        )
      }
    }
  })

  test(
    "CCDIIncludeRequestedSubmissionId",
    "The requested submission ID is present in the associated completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val requestedSubmissionIds =
      for (_ <- 1 to 2)
        yield Ref.SubmissionId.assertFromString(SubmissionIdGenerator.Random.generate())
    val Seq(successfulRequestSubmissionId, failingRequestSubmissionId) = requestedSubmissionIds
    for {
      _ <- submitSuccessfulAndFailingRequests(
        service,
        ledger,
        party,
        updateSuccessfulCommandServiceRequest =
          updateSubmitAndWaitRequest(_, optSubmissionId = Some(successfulRequestSubmissionId)),
        updateFailingCommandServiceRequest =
          updateSubmitAndWaitRequest(_, optSubmissionId = Some(failingRequestSubmissionId)),
        updateSuccessfulCommandSubmissionServiceRequest =
          updateSubmitRequest(_, optSubmissionId = Some(successfulRequestSubmissionId)),
        updateFailingCommandSubmissionServiceRequest =
          updateSubmitRequest(_, optSubmissionId = Some(failingRequestSubmissionId)),
      )
      completions <- ledger.firstCompletions(party)
    } yield {
      assertCompletionsCount(completions, 2)
      completions.zip(requestedSubmissionIds).foreach { case (completion, expectedSubmissionId) =>
        val actualSubmissionId = completion.submissionId
        assert(
          actualSubmissionId == expectedSubmissionId,
          "Wrong submission ID in completion, " +
            s"expected: $expectedSubmissionId, actual: $actualSubmissionId",
        )
      }
    }
  })

  test(
    "CCDIIncludeASubmissionIdWhenNotRequested",
    "A completion includes a submission ID when one is missing in the request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- submitSuccessfulAndFailingRequests(service, ledger, party)
      completions <- ledger.firstCompletions(party)
    } yield {
      assertCompletionsCount(completions, 2)
      completions.foreach { completion =>
        assert(
          Ref.SubmissionId.fromString(completion.submissionId).isRight,
          "Missing or invalid submission ID in completion",
        )
      }
    }
  })

  test(
    "CCDIIncludeRequestedDeduplicationOffset",
    "The requested deduplication offset is present in the associated completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val submittedDeduplications =
      for (i <- 1 to 2) yield DeduplicationPeriod.DeduplicationOffset(s"offset$i")
    val Seq(successfulRequestDeduplication, failingRequestDeduplication) = submittedDeduplications
    for {
      _ <- submitSuccessfulAndFailingRequests(
        service,
        ledger,
        party,
        updateSuccessfulCommandServiceRequest = updateSubmitAndWaitRequest(
          _,
          optDeduplicationPeriod = Some(successfulRequestDeduplication),
        ),
        updateFailingCommandServiceRequest =
          updateSubmitAndWaitRequest(_, optDeduplicationPeriod = Some(failingRequestDeduplication)),
        updateSuccessfulCommandSubmissionServiceRequest =
          updateSubmitRequest(_, optDeduplicationPeriod = Some(successfulRequestDeduplication)),
        updateFailingCommandSubmissionServiceRequest =
          updateSubmitRequest(_, optDeduplicationPeriod = Some(failingRequestDeduplication)),
      )
      completions <- ledger.firstCompletions(party)
    } yield {
      assertCompletionsCount(completions, 2)
      completions.zip(submittedDeduplications).foreach {
        case (completion, requestedDeduplication) =>
          val expectedDeduplicationOffset = completion.deduplicationPeriod.deduplicationOffset
          val actualDeduplicationOffset = requestedDeduplication.deduplicationOffset
          assert(
            expectedDeduplicationOffset == actualDeduplicationOffset,
            "Wrong duplication offset in completion, " +
              s"expected: $expectedDeduplicationOffset, " +
              s"actual: $actualDeduplicationOffset",
          )
      }
    }
  })

  test(
    "CCDIIncludeRequestedDeduplicationTime",
    "The requested deduplication time is present in the associated completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val submittedDeduplications =
      for (i <- 1 to 2)
        yield DeduplicationPeriod.DeduplicationTime(
          Duration(seconds = 100L + i.toLong, nanos = 10 + i)
        )
    val Seq(successfulRequestDeduplication, failingRequestDeduplication) = submittedDeduplications
    for {
      _ <- submitSuccessfulAndFailingRequests(
        service,
        ledger,
        party,
        updateSuccessfulCommandServiceRequest = updateSubmitAndWaitRequest(
          _,
          optDeduplicationPeriod = Some(successfulRequestDeduplication),
        ),
        updateFailingCommandServiceRequest =
          updateSubmitAndWaitRequest(_, optDeduplicationPeriod = Some(failingRequestDeduplication)),
        updateSuccessfulCommandSubmissionServiceRequest =
          updateSubmitRequest(_, optDeduplicationPeriod = Some(successfulRequestDeduplication)),
        updateFailingCommandSubmissionServiceRequest =
          updateSubmitRequest(_, optDeduplicationPeriod = Some(failingRequestDeduplication)),
      )
      commandSubmissionCompletions <- ledger.firstCompletions(party)
    } yield {
      assertCompletionsCount(commandSubmissionCompletions, 2)
      commandSubmissionCompletions.zip(submittedDeduplications).foreach {
        case (completion, requestedDeduplication) =>
          val expectedDeduplicationTime = requestedDeduplication.deduplicationTime
          val actualDeduplicationTime = completion.deduplicationPeriod.deduplicationTime
          assert(
            actualDeduplicationTime == expectedDeduplicationTime,
            "Wrong duplication time in completion, " +
              s"expected: $expectedDeduplicationTime, " +
              s"actual: $actualDeduplicationTime",
          )
      }
    }
  })

  test(
    "CCDIIncludeNoDeduplicationWhenNotRequested",
    "A completion doesn't include a deduplication when one is missing in the request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- submitSuccessfulAndFailingRequests(service, ledger, party)
      commandSubmissionCompletions <- ledger.firstCompletions(party)
    } yield {
      assertCompletionsCount(commandSubmissionCompletions, 2)
      commandSubmissionCompletions.foreach { completion =>
        val actualDeduplication = completion.deduplicationPeriod
        assert(
          actualDeduplication.isEmpty,
          s"The deduplication $actualDeduplication " +
            "is present in the completion even though it was not requested",
        )
      }
    }
  })
}

private[testtool] object CompletionDeduplicationInfoIT {
  sealed trait Service extends Serializable with Product
  case object CommandService extends Service
  case object CommandSubmissionService extends Service

  private def submitSuccessfulAndFailingRequests(
      service: Service,
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      updateSuccessfulCommandServiceRequest: SubmitAndWaitRequest => SubmitAndWaitRequest =
        identity,
      updateFailingCommandServiceRequest: SubmitAndWaitRequest => SubmitAndWaitRequest = identity,
      updateSuccessfulCommandSubmissionServiceRequest: SubmitRequest => SubmitRequest = identity,
      updateFailingCommandSubmissionServiceRequest: SubmitRequest => SubmitRequest = identity,
  )(implicit ec: ExecutionContext): Future[Unit] =
    service match {
      case CommandService =>
        val successfulRequest = ledger.submitAndWaitRequest(party, successfulCommand(party))
        val failingRequest = ledger.submitAndWaitRequest(party, failingCommand(party))
        for {
          _ <- ledger.submitAndWait(updateSuccessfulCommandServiceRequest(successfulRequest))
          _ <- ledger.submitAndWait(updateFailingCommandServiceRequest(failingRequest))
        } yield ()
      case CommandSubmissionService =>
        val successfulRequest = ledger.submitRequest(party, successfulCommand(party))
        val failingRequest = ledger.submitRequest(party, failingCommand(party))
        for {
          _ <- ledger.submit(updateSuccessfulCommandSubmissionServiceRequest(successfulRequest))
          _ <- ledger.submit(updateFailingCommandSubmissionServiceRequest(failingRequest))
        } yield ()
    }

  private def updateSubmitRequest(
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

  private def updateSubmitAndWaitRequest(
      submitAndWaitRequest: SubmitAndWaitRequest,
      optSubmissionId: Option[Ref.SubmissionId] = None,
      optDeduplicationPeriod: Option[DeduplicationPeriod] = None,
  ): SubmitAndWaitRequest = {
    val optRequestUpdatedWithSubmission =
      optSubmissionId.map { submissionId =>
        submitAndWaitRequest.copy(commands =
          submitAndWaitRequest.commands.map(_.copy(submissionId = submissionId))
        )
      }

    val optRequestUpdatedWithDeduplicationPeriod =
      optDeduplicationPeriod.map { deduplicationPeriod =>
        optRequestUpdatedWithSubmission
          .getOrElse(submitAndWaitRequest)
          .copy(commands =
            submitAndWaitRequest.commands.map(_.copy(deduplicationPeriod = deduplicationPeriod))
          )
      }

    optRequestUpdatedWithDeduplicationPeriod.getOrElse(submitAndWaitRequest)
  }
  private def assertCompletionsCount(
      completions: Seq[Completion],
      expectedCount: Int,
  ): Seq[Completion] =
    assertLength(
      context =
        s"Completions count mismatch, expected: $expectedCount, actual: ${completions.size}",
      expectedCount,
      completions,
    )

  private def successfulCommand(party: Primitive.Party) = Dummy(party).create.command

  private def failingCommand(party: Primitive.Party) =
    Dummy(party).createAnd.exerciseFailingChoice(party).command
}
