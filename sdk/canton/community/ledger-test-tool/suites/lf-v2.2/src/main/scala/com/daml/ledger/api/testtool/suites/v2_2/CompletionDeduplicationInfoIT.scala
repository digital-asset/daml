// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertDefined
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party, WithTimeout}
import com.daml.ledger.api.testtool.suites.v2_2.CompletionDeduplicationInfoIT.*
import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.javaapi.data.Command
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.SubmissionId
import io.grpc.Status

import java.util.List as JList
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

final class CompletionDeduplicationInfoIT[ServiceRequest](
    service: Service[ServiceRequest]
) extends LedgerTestSuite {

  private val serviceName: String = service.productPrefix

  override def name = super.name

  test(
    shortIdentifier = s"CCDIIncludeDedupInfo$serviceName",
    description = s"Deduplication information is preserved in completions ($serviceName)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
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
      assertUserIdIsPreserved(ledger.userId, optNoDeduplicationSubmittedCompletion)
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
        party: Party,
        optSubmissionId: Option[Ref.SubmissionId] = None,
    ): ProtoRequestType

    def submitRequest(
        ledger: ParticipantTestContext,
        party: Party,
        request: ProtoRequestType,
    )(implicit ec: ExecutionContext): Future[Option[Completion]]

    def assertCompletion(optCompletion: Option[Completion]): Unit
  }

  case object CommandService extends Service[SubmitAndWaitRequest] {
    override def buildRequest(
        ledger: ParticipantTestContext,
        party: Party,
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
        party: Party,
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
        party: Party,
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
        party: Party,
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
      party: Party,
      offset: Long,
  ): Future[Option[Completion]] =
    WithTimeout(5.seconds)(
      ledger
        .findCompletion(ledger.completionStreamRequest(offset)(party))(_ => true)
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

  private def assertUserIdIsPreserved(
      requestedUserId: String,
      optCompletion: Option[Completion],
  ): Unit = {
    val expectedUserId = requestedUserId
    assertDefined(optCompletion, "No completion has been produced").discard
    val userIdCompletion = optCompletion.get
    assert(userIdCompletion.status.forall(_.code == Status.Code.OK.value()))
    val actualUserId = userIdCompletion.userId
    assert(
      Ref.UserId.fromString(actualUserId).contains(expectedUserId),
      "Wrong user ID in completion, " +
        s"expected: $expectedUserId, actual: $actualUserId",
    )
  }

  private def simpleCreate(party: Party): JList[Command] = new Dummy(party.getValue).create.commands

  private val RandomSubmissionId =
    Ref.SubmissionId.assertFromString(SubmissionIdGenerator.Random.generate())
}
