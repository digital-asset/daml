// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.util.UUID

import com.daml.grpc.GrpcException
import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.fail
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.Test.{Dummy, DummyWithAnnotation}
import io.grpc.Status
import io.grpc.Status.Code

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/** Should be enabled for ledgers that fill the submission ID in the completions,
  * as we need to use the submission id to find completions for parallel submissions
  */
class AppendOnlyCommandDeduplicationParallelIT extends LedgerTestSuite {

  test(
    s"DeduplicateParallelSubmissionsUsingCommandSubmissionService",
    "Commands submitted at the same, in parallel,  should be deduplicated",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val deduplicationDuration = 3.seconds
    lazy val request = ledger
      .submitRequest(party, DummyWithAnnotation(party, "Duplicate").create.command)
      .update(
        _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationDuration(
          deduplicationDuration.asProtobuf
        )
      )
    runTestWithSubmission(
      request,
      submitRequestAndGetStatusCode(ledger)(_, party),
    )
  })

  test(
    s"DeduplicateParallelSubmissionsUsingCommandService",
    "Commands submitted at the same, in parallel, should be deduplicated",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val deduplicationDuration = 3.seconds
    val request = ledger
      .submitAndWaitRequest(party, Dummy(party).create.command)
      .update(
        _.commands.deduplicationDuration := deduplicationDuration.asProtobuf
      )
    runTestWithSubmission(
      request,
      submitAndWaitRequestAndGetStatusCode(ledger)(_, party),
    )
  })

  private def runTestWithSubmission[T](request: T, submitRequestAndGetStatus: T => Future[Code])(
      implicit ec: ExecutionContext
  ) = {
    val numberOfParallelRequests = 10
    Future
      .traverse(Seq.fill(numberOfParallelRequests)(request))(request => {
        submitRequestAndGetStatus(request)
      })
      .map(_.groupBy(identity).view.mapValues(_.size).toMap)
      .map(responses => {
        val expectedDuplicateResponses = numberOfParallelRequests - 1
        val okResponses = responses.getOrElse(Code.OK, 0)
        val alreadyExistsResponses = responses.getOrElse(Code.ALREADY_EXISTS, 0)
        // Participant-based command de-duplication can currently also reject duplicates via a SQL exception
        val internalResponses = responses.getOrElse(Code.INTERNAL, 0)
        // Canton can return ABORTED for duplicate submissions
        val abortedResponses = responses.getOrElse(Code.ABORTED, 0)
        val duplicateResponses =
          alreadyExistsResponses + internalResponses + abortedResponses
        assert(
          okResponses == 1 && duplicateResponses == numberOfParallelRequests - 1,
          s"Expected $expectedDuplicateResponses duplicate responses and one accepted, got $responses",
        )
      })
  }

  private def submitAndWaitRequestAndGetStatusCode(
      ledger: ParticipantTestContext
  )(request: SubmitAndWaitRequest, parties: Party*)(implicit ec: ExecutionContext) = {
    val submissionId = UUID.randomUUID().toString
    val requestWithSubmissionId = request.update(_.commands.submissionId := submissionId)
    val submitResult = ledger.submitAndWait(requestWithSubmissionId)
    submissionResultToFinalStatusCode(ledger)(submitResult, submissionId, parties: _*)
  }
  protected def submitRequestAndGetStatusCode(
      ledger: ParticipantTestContext
  )(request: SubmitRequest, parties: Party*)(implicit ec: ExecutionContext): Future[Code] = {
    val submissionId = UUID.randomUUID().toString
    val requestWithSubmissionId = request.update(_.commands.submissionId := submissionId)
    val submitResult = ledger
      .submit(requestWithSubmissionId)
    submissionResultToFinalStatusCode(ledger)(submitResult, submissionId, parties: _*)
  }

  private def submissionResultToFinalStatusCode(
      ledger: ParticipantTestContext
  )(submitResult: Future[Unit], submissionId: String, parties: Primitive.Party*)(implicit
      ec: ExecutionContext
  ) = {
    submitResult
      .flatMap(_ => ledger.findCompletion(parties: _*)(_.submissionId == submissionId))
      .map {
        case Some(completion) =>
          completion.getStatus.code
        case None => fail(s"Did not find completion for request with submission id $submissionId")
      }
      .recover {
        case GrpcException(status, _) =>
          status.getCode.value()
        case otherException => fail("Not a GRPC exception", otherException)
      }
      .map(codeValue => Status.fromCodeValue(codeValue).getCode)
  }
}
