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
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy
import com.daml.lf.data.Ref
import com.daml.platform.testing.WithTimeout
import com.google.protobuf.duration.Duration
import io.grpc.Status

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

final class CompletionDeduplicationInfoIT(service: Service) extends LedgerTestSuite {

  private val serviceName: String = service.productPrefix

  override private[testtool] def name = serviceName + super.name

  test(
    shortIdentifier = s"CCDIIncludeDedupInfo-$serviceName",
    description = s"Deduplication information is preserved in completions ($serviceName)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      config <- ledger.configuration()
      optApplicationIdCompletion <- submitRequest(service, ledger, party, simpleCreate(party))
      optSubmissionIdCompletion <- submitRequest(
        service,
        ledger,
        party,
        simpleCreate(party),
        updateCommandServiceRequest = _.update(_.commands.submissionId := aSubmissionId),
        updateCommandSubmissionServiceRequest = _.update(_.commands.submissionId := aSubmissionId),
      )
      optCompletionDeduplicationTime <- submitRequest(
        service,
        ledger,
        party,
        simpleCreate(party),
        updateCommandServiceRequest = _.update(_.commands.deduplicationTime := aDeduplicationTime),
        updateCommandSubmissionServiceRequest =
          _.update(_.commands.deduplicationTime := aDeduplicationTime),
      )
    } yield {
      assertApplicationIdIsPreserved(ledger.applicationId, optApplicationIdCompletion)
      assertSubmissionIdIsGenerated(optApplicationIdCompletion)
      assertDefaultDeduplicationTimeIsReported(config, optApplicationIdCompletion)
      assertSubmissionIdIsPreserved(aSubmissionId, optSubmissionIdCompletion)
      assertDeduplicationTimeIsPreserved(aDeduplicationTime, optCompletionDeduplicationTime)
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

  private def assertDeduplicationTimeIsPreserved(
      requestedDeduplicationTime: Duration,
      optCompletion: Option[Completion],
  ): Unit = {
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

  private def assertSubmissionIdIsPreserved(
      requestedSubmissionId: Ref.SubmissionId,
      optCompletion: Option[Completion],
  ): Unit = {
    val submissionIdCompletion = assertDefined(optCompletion)
    val actualSubmissionId = submissionIdCompletion.submissionId
    assert(submissionIdCompletion.status.forall(_.code == Status.Code.OK.value()))
    assert(
      actualSubmissionId == requestedSubmissionId,
      "Wrong submission ID in completion, " +
        s"expected: $requestedSubmissionId, actual: $actualSubmissionId",
    )
  }

  private def assertDefaultDeduplicationTimeIsReported(
      config: LedgerConfiguration,
      optCompletion: Option[Completion],
  ): Unit = {
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

  private def assertSubmissionIdIsGenerated(optCompletion: Option[Completion]): Unit = {
    val completion = assertDefined(optCompletion)
    assert(completion.status.forall(_.code == Status.Code.OK.value()))
    assert(
      Ref.SubmissionId.fromString(completion.submissionId).isRight,
      "Missing or invalid submission ID in completion",
    )
  }

  private def assertApplicationIdIsPreserved(
      requestedApplicationId: String,
      optCompletion: Option[Completion],
  ): Unit = {
    val expectedApplicationId = requestedApplicationId
    assertDefined(optCompletion)
    val applicationIdCompletion = optCompletion.get
    assert(applicationIdCompletion.status.forall(_.code == Status.Code.OK.value()))
    val actualApplicationId = applicationIdCompletion.applicationId
    assert(
      Ref.ApplicationId.fromString(actualApplicationId).contains(expectedApplicationId),
      "Wrong application ID in completion, " +
        s"expected: $expectedApplicationId, actual: $actualApplicationId",
    )
  }

  private def assertDefined(optCompletion: Option[Completion]): Completion = {
    assert(optCompletion.isDefined, "No completion has been produced")
    optCompletion.get
  }

  private def simpleCreate(party: Primitive.Party): Command = Dummy(party).create.command

  private val aSubmissionId =
    Ref.SubmissionId.assertFromString(SubmissionIdGenerator.Random.generate())

  private val aDeduplicationTime = Duration(seconds = 100L, nanos = 10)
}
