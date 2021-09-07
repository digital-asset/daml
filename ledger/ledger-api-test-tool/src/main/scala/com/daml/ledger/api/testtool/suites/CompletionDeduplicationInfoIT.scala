// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.suites.CompletionDeduplicationInfoIT._
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy
import com.daml.lf.data.Ref
import com.daml.platform.testing.WithTimeout
import com.google.protobuf.duration.Duration
import io.grpc.Status

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

final class CompletionDeduplicationInfoIT(service: Service) extends LedgerTestSuite {

  override private[testtool] def name = service.productPrefix + super.name

  test(
    shortIdentifier = service.productPrefix + "CCDIIncludeApplicationId",
    "The application ID is present in completions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      optCompletion <- submitRequest(service, ledger, party, simpleCreate(party))
    } yield {
      val expectedApplicationId = ledger.applicationId
      assertDefined(optCompletion)
      val completion = optCompletion.get
      assert(completion.status.forall(_.code == Status.Code.OK.value()))
      val actualApplicationId = completion.applicationId
      assert(
        Ref.ApplicationId.fromString(actualApplicationId).contains(expectedApplicationId),
        "Wrong application ID in completion, " +
          s"expected: $expectedApplicationId, actual: $actualApplicationId",
      )
    }
  })

  test(
    shortIdentifier = service.productPrefix + "CCDIIncludeRequestedSubmissionId",
    "The requested submission ID is present in the associated completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val requestedSubmissionId =
      Ref.SubmissionId.assertFromString(SubmissionIdGenerator.Random.generate())
    for {
      optCompletion <- submitRequest(
        service,
        ledger,
        party,
        simpleCreate(party),
        updateCommandServiceRequest = _.update(_.commands.submissionId := requestedSubmissionId),
        updateCommandSubmissionServiceRequest =
          _.update(_.commands.submissionId := requestedSubmissionId),
      )
    } yield {
      val completion = assertDefined(optCompletion)
      val actualSubmissionId = completion.submissionId
      assert(completion.status.forall(_.code == Status.Code.OK.value()))
      assert(
        actualSubmissionId == requestedSubmissionId,
        "Wrong submission ID in completion, " +
          s"expected: $requestedSubmissionId, actual: $actualSubmissionId",
      )
    }
  })

  test(
    shortIdentifier = service.productPrefix + "CCDIIncludeASubmissionIdWhenNotRequested",
    "A completion includes a submission ID when one is missing in the request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      optCompletion <- submitRequest(service, ledger, party, simpleCreate(party))
    } yield {
      val completion = assertDefined(optCompletion)
      assert(completion.status.forall(_.code == Status.Code.OK.value()))
      assert(
        Ref.SubmissionId.fromString(completion.submissionId).isRight,
        "Missing or invalid submission ID in completion",
      )
    }
  })

  test(
    shortIdentifier = service.productPrefix + "CCDIIncludeRequestedDeduplicationTime",
    "The requested deduplication time is present in the associated completion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val requestedDeduplicationTime = Duration(seconds = 100L, nanos = 10)
    for {
      optCompletion <- submitRequest(
        service,
        ledger,
        party,
        simpleCreate(party),
        updateCommandServiceRequest =
          _.update(_.commands.deduplicationTime := requestedDeduplicationTime),
        updateCommandSubmissionServiceRequest =
          _.update(_.commands.deduplicationTime := requestedDeduplicationTime),
      )
    } yield {
      val completion = assertDefined(optCompletion)
      val expectedDeduplicationTime = Some(requestedDeduplicationTime)
      val actualDeduplicationTime = completion.deduplicationPeriod.deduplicationTime
      assert(completion.status.forall(_.code == Status.Code.OK.value()))
      assert(
        actualDeduplicationTime == expectedDeduplicationTime,
        "Wrong duplication time in completion, " +
          s"expected: $expectedDeduplicationTime, " +
          s"actual: $actualDeduplicationTime",
      )
    }
  })

  test(
    shortIdentifier = service.productPrefix + "CCDIIncludeNoDeduplicationWhenNotRequested",
    "A completion includes the max deduplication time when one is missing in the request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      config <- ledger.configuration()
      optCompletion <- submitRequest(service, ledger, party, simpleCreate(party))
    } yield {
      val completion = assertDefined(optCompletion)
      assert(completion.status.forall(_.code == Status.Code.OK.value()))
      val actualDeduplication = completion.deduplicationPeriod
      assert(actualDeduplication.isDeduplicationTime)
      assert(
        actualDeduplication.deduplicationTime == config.maxDeduplicationTime,
        s"The deduplication $actualDeduplication " +
          "is not the maximum deduplication time",
      )
    }
  })
}

private[testtool] object CompletionDeduplicationInfoIT {
  sealed trait Service extends Serializable with Product
  case object CommandService extends Service
  case object CommandSubmissionService extends Service

  private def submitRequest(
      service: Service,
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      command: Command,
      updateCommandServiceRequest: SubmitAndWaitRequest => SubmitAndWaitRequest = identity,
      updateCommandSubmissionServiceRequest: SubmitRequest => SubmitRequest = identity,
  )(implicit ec: ExecutionContext): Future[Option[Completion]] =
    service match {
      case CommandService =>
        val successfulRequest = ledger.submitAndWaitRequest(party, command)
        for {
          offset <- ledger.currentEnd()
          _ <- ledger.submitAndWait(updateCommandServiceRequest(successfulRequest))
          completion <- WithTimeout(5.seconds)(
            ledger.findCompletion(ledger.completionStreamRequest(offset)(party))(_ => true)
          )
        } yield completion
      case CommandSubmissionService =>
        val successfulRequest = ledger.submitRequest(party, command)
        for {
          offset <- ledger.currentEnd()
          _ <- ledger.submit(updateCommandSubmissionServiceRequest(successfulRequest))
          completion <- WithTimeout(5.seconds)(
            ledger.findCompletion(ledger.completionStreamRequest(offset)(party))(_ => true)
          )
        } yield completion
    }

  private def assertDefined(optCompletion: Option[Completion]): Completion = {
    assert(optCompletion.isDefined, "No completion has been produced")
    optCompletion.get
  }

  private def simpleCreate(party: Primitive.Party): Command = Dummy(party).create.command
}
