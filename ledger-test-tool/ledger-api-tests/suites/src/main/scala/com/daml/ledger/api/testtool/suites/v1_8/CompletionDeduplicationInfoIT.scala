// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertDefined
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.suites.v1_8.CompletionDeduplicationInfoIT._
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.completion.Completion
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

final class CompletionDeduplicationInfoIT[ServiceRequest](
    service: Service[ServiceRequest]
) extends LedgerTestSuite {

  private val serviceName: String = service.productPrefix

  override def name = super.name + serviceName

  test(
    shortIdentifier = s"CCDIIncludeDedupInfo$serviceName",
    description = s"Deduplication information is preserved in completions ($serviceName)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val requestWithoutSubmissionId = service.buildRequest(ledger, party)
    val requestWithSubmissionId = service.buildRequest(ledger, party, Some(RandomSubmissionId))
    for {
      optNoDeduplicationSubmittedCompletion <- service.submitRequest(
        ledger,
        party,
        requestWithoutSubmissionId,
      )
      optSubmissionIdSubmittedCompletion <- service
        .submitRequest(ledger, party, requestWithSubmissionId)
    } yield {
      assertApplicationIdIsPreserved(ledger.applicationId, optNoDeduplicationSubmittedCompletion)
      service.assertCompletion(optNoDeduplicationSubmittedCompletion)
      assertDeduplicationPeriodIsReported(optNoDeduplicationSubmittedCompletion)
      assertSubmissionIdIsPreserved(optSubmissionIdSubmittedCompletion, RandomSubmissionId)
    }
  })
}

private[testtool] object CompletionDeduplicationInfoIT {

  private[testtool] sealed trait Service[ProtoRequestType] extends Serializable with Product {
    def buildRequest(
        ledger: ParticipantTestContext,
        party: Primitive.Party,
        optSubmissionId: Option[Ref.SubmissionId] = None,
    ): ProtoRequestType

    def submitRequest(
        ledger: ParticipantTestContext,
        party: Primitive.Party,
        request: ProtoRequestType,
    )(implicit ec: ExecutionContext): Future[Option[Completion]]

    def assertCompletion(optCompletion: Option[Completion]): Unit
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
        party: binding.Primitive.Party,
        request: SubmitAndWaitRequest,
    )(implicit ec: ExecutionContext): Future[Option[Completion]] =
      for {
        offset <- ledger.currentEnd()
        _ <- ledger.submitAndWait(request)
        completion <- singleCompletionAfterOffset(ledger, party, offset)
      } yield completion

    override def assertCompletion(optCompletion: Option[Completion]): Unit = {
      val completion = assertDefined(optCompletion, "No completion has been produced")
      assert(completion.status.forall(_.code == Status.Code.OK.value()))
      assert(
        Ref.SubmissionId.fromString(completion.submissionId).isRight,
        "Missing or invalid submission ID in completion",
      )
    }
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
        party: binding.Primitive.Party,
        request: SubmitRequest,
    )(implicit ec: ExecutionContext): Future[Option[Completion]] =
      for {
        offset <- ledger.currentEnd()
        _ <- ledger.submit(request)
        completion <- singleCompletionAfterOffset(ledger, party, offset)
      } yield completion

    override def assertCompletion(optCompletion: Option[Completion]): Unit = {
      val completion = assertDefined(optCompletion, "No completion has been produced")
      assert(completion.status.forall(_.code == Status.Code.OK.value()))
    }
  }

  private def singleCompletionAfterOffset(
      ledger: ParticipantTestContext,
      party: binding.Primitive.Party,
      offset: LedgerOffset,
  )(implicit ec: ExecutionContext): Future[Option[Completion]] =
    WithTimeout(5.seconds)(
      ledger
        .findCompletion(ledger.completionStreamRequest(offset)(party))(_ => true)
        .map(_.map(_.completion))
    )

  private def assertSubmissionIdIsPreserved(
      optCompletion: Option[Completion],
      requestedSubmissionId: Ref.SubmissionId,
  ): Unit = {
    val submissionIdCompletion = assertDefined(optCompletion, "No completion has been produced")
    val actualSubmissionId = submissionIdCompletion.submissionId
    assert(submissionIdCompletion.status.forall(_.code == Status.Code.OK.value()))
    assert(
      actualSubmissionId == requestedSubmissionId,
      "Wrong submission ID in completion, " +
        s"expected: $requestedSubmissionId, actual: $actualSubmissionId",
    )
  }

  private def assertDeduplicationPeriodIsReported(
      optCompletion: Option[Completion]
  ): Unit = {
    val completion = assertDefined(optCompletion, "No completion has been produced")
    assert(completion.status.forall(_.code == Status.Code.OK.value()))
    assert(completion.deduplicationPeriod.isDefined, "The deduplication period was not reported")
  }

  private def assertApplicationIdIsPreserved(
      requestedApplicationId: String,
      optCompletion: Option[Completion],
  ): Unit = {
    val expectedApplicationId = requestedApplicationId
    assertDefined(optCompletion, "No completion has been produced")
    val applicationIdCompletion = optCompletion.get
    assert(applicationIdCompletion.status.forall(_.code == Status.Code.OK.value()))
    val actualApplicationId = applicationIdCompletion.applicationId
    assert(
      Ref.ApplicationId.fromString(actualApplicationId).contains(expectedApplicationId),
      "Wrong application ID in completion, " +
        s"expected: $expectedApplicationId, actual: $actualApplicationId",
    )
  }

  private def simpleCreate(party: Primitive.Party): Command = Dummy(party).create.command

  private val RandomSubmissionId =
    Ref.SubmissionId.assertFromString(SubmissionIdGenerator.Random.generate())
}
