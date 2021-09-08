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
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.SubmissionId
import com.daml.platform.testing.WithTimeout
import io.grpc.Status

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

final class CompletionDeduplicationInfoIT[ServiceRequest](service: Service[ServiceRequest])
    extends LedgerTestSuite {

  private val serviceName: String = service.productPrefix

  override private[testtool] def name = serviceName + super.name

  test(
    shortIdentifier = s"CCDIIncludeDedupInfo$serviceName",
    description = s"Deduplication information is preserved in completions ($serviceName)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      config <- ledger.configuration()
      requestWithoutSubmissionId = service.buildRequest(ledger, party)
      optApplicationIdCompletion <- service.submitRequest(ledger, requestWithoutSubmissionId)
      requestWithSubmissionId = service.buildRequest(ledger, party, Some(RandomSubmissionId))
      optSubmissionIdCompletion <- service.submitRequest(ledger, requestWithSubmissionId)
    } yield {
      assertApplicationIdIsPreserved(ledger.applicationId, optApplicationIdCompletion)
      assertSubmissionIdIsGenerated(optApplicationIdCompletion)
      assertDefaultDeduplicationTimeIsReportedIfNoDeduplicationSpecified(
        config,
        optApplicationIdCompletion,
      )
      assertSubmissionIdIsPreserved(RandomSubmissionId, optSubmissionIdCompletion)
    }
  })
}

private[testtool] object CompletionDeduplicationInfoIT {

  sealed trait Service[ProtoRequestType] extends Serializable with Product {
    def buildRequest(
        ledger: ParticipantTestContext,
        party: Primitive.Party,
        optSubmissionId: Option[Ref.SubmissionId] = None,
    ): ProtoRequestType

    def submitRequest(
        ledger: ParticipantTestContext,
        request: ProtoRequestType,
    )(implicit ec: ExecutionContext): Future[Option[Completion]]
  }

  case object CommandService extends Service[SubmitAndWaitRequest] {
    override def buildRequest(
        ledger: ParticipantTestContext,
        party: binding.Primitive.Party,
        optSubmissionId: Option[SubmissionId],
    ): SubmitAndWaitRequest = {
      val request = ledger.submitAndWaitRequest(party, simpleCreate(party))
      optSubmissionId
        .map { submissionId =>
          request.update(_.commands.submissionId := submissionId)
        }
        .getOrElse(request)
    }

    override def submitRequest(
        ledger: ParticipantTestContext,
        request: SubmitAndWaitRequest,
    )(implicit ec: ExecutionContext): Future[Option[Completion]] =
      for {
        offset <- ledger.currentEnd()
        _ <- ledger.submitAndWait(request)
        completion <- singleCompletionAfterOffset(ledger, offset)
      } yield completion
  }

  case object CommandSubmissionService extends Service[SubmitRequest] {
    override def buildRequest(
        ledger: ParticipantTestContext,
        party: binding.Primitive.Party,
        optSubmissionId: Option[SubmissionId],
    ): SubmitRequest = {
      val request = ledger.submitRequest(party, simpleCreate(party))
      optSubmissionId
        .map { submissionId =>
          request.update(_.commands.submissionId := submissionId)
        }
        .getOrElse(request)
    }

    override def submitRequest(
        ledger: ParticipantTestContext,
        request: SubmitRequest,
    )(implicit ec: ExecutionContext): Future[Option[Completion]] =
      for {
        offset <- ledger.currentEnd()
        _ <- ledger.submit(request)
        completion <- singleCompletionAfterOffset(ledger, offset)
      } yield completion
  }

  private def singleCompletionAfterOffset(
      ledger: ParticipantTestContext,
      offset: LedgerOffset,
  ): Future[Option[Completion]] =
    WithTimeout(5.seconds)(
      ledger.findCompletion(ledger.completionStreamRequest(offset)())(_ => true)
    )

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

  private def assertDefaultDeduplicationTimeIsReportedIfNoDeduplicationSpecified(
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

  private val RandomSubmissionId =
    Ref.SubmissionId.assertFromString(SubmissionIdGenerator.Random.generate())
}
