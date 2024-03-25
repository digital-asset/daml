// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

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
import com.daml.ledger.javaapi.data.Party
import com.daml.ledger.test.java.model.test.DummyWithAnnotation
import io.grpc.Status
import io.grpc.Status.Code

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success}

class CommandDeduplicationParallelIT extends LedgerTestSuite {

  private val deduplicationDuration = 3.seconds
  private val numberOfParallelRequests = 10

  test(
    "DeduplicateParallelSubmissionsUsingCommandSubmissionService",
    "Commands submitted at the same, in parallel, using the CommandSubmissionService, should be deduplicated",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    lazy val request = buildSubmitRequest(ledger, party)
    runTestWithSubmission[SubmitRequest](
      ledger,
      party,
      () => submitRequestAndGetStatusCode(ledger)(request, party),
    )
  })

  test(
    "DeduplicateParallelSubmissionsUsingCommandService",
    "Commands submitted at the same, in parallel, using the CommandService, should be deduplicated",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = buildSubmitAndWaitRequest(ledger, party)
    runTestWithSubmission[SubmitAndWaitRequest](
      ledger,
      party,
      () => submitAndWaitRequestAndGetStatusCode(ledger)(request, party),
    )
  })

  test(
    "DeduplicateParallelSubmissionsUsingMixedCommandServiceAndCommandSubmissionService",
    "Commands submitted at the same, in parallel, using the CommandService and the CommandSubmissionService, should be deduplicated",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val submitAndWaitRequest = buildSubmitAndWaitRequest(ledger, party)
    val submitRequest = buildSubmitRequest(ledger, party).update(
      _.commands.commandId := submitAndWaitRequest.getCommands.commandId
    )
    runTestWithSubmission[SubmitAndWaitRequest](
      ledger,
      party,
      () =>
        if (Random.nextBoolean())
          submitAndWaitRequestAndGetStatusCode(ledger)(submitAndWaitRequest, party)
        else submitRequestAndGetStatusCode(ledger)(submitRequest, party),
    )
  })

  private def runTestWithSubmission[T](
      ledger: ParticipantTestContext,
      party: Party,
      submitRequestAndGetStatus: () => Future[Code],
  )(implicit
      ec: ExecutionContext
  ) = {
    for {
      responses <- Future
        .traverse(Seq.fill(numberOfParallelRequests)(()))(_ => {
          submitRequestAndGetStatus()
        })
        .map(_.groupBy(identity).view.mapValues(_.size).toMap)
      activeContracts <- ledger.activeContracts(party)
    } yield {
      val expectedDuplicateResponses = numberOfParallelRequests - 1
      val okResponses = responses.getOrElse(Code.OK, 0)
      val alreadyExistsResponses = responses.getOrElse(Code.ALREADY_EXISTS, 0)
      // Canton can return ABORTED for parallel in-flight duplicate submissions
      val abortedResponses = responses.getOrElse(Code.ABORTED, 0)
      val duplicateResponses =
        if (ledger.features.commandDeduplicationFeatures.deduplicationType.isAsyncAndConcurrentSync)
          alreadyExistsResponses + abortedResponses
        else alreadyExistsResponses
      assert(
        okResponses == 1 && duplicateResponses == numberOfParallelRequests - 1,
        s"Expected $expectedDuplicateResponses duplicate responses and one accepted, got $responses",
      )
      assert(activeContracts.size == 1)
    }
  }

  private def buildSubmitRequest(
      ledger: ParticipantTestContext,
      party: Party,
  ) = ledger
    .submitRequest(
      party,
      new DummyWithAnnotation(party, "Duplicate Using CommandSubmissionService").create.commands,
    )
    .update(
      _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationDuration(
        deduplicationDuration.asProtobuf
      )
    )

  private def buildSubmitAndWaitRequest(
      ledger: ParticipantTestContext,
      party: Party,
  ) = ledger
    .submitAndWaitRequest(
      party,
      new DummyWithAnnotation(party, "Duplicate using CommandService").create.commands,
    )
    .update(
      _.commands.deduplicationDuration := deduplicationDuration.asProtobuf
    )

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
  )(submitResult: Future[Unit], submissionId: String, parties: Party*)(implicit
      ec: ExecutionContext
  ) = submitResult
    .transformWith {
      case Failure(exception) =>
        exception match {
          case GrpcException(status, _) =>
            Future.successful(status.getCode)
          case NonFatal(otherException) =>
            fail(s"Not a GRPC exception $otherException", otherException)
        }
      case Success(_) =>
        ledger
          .findCompletion(parties: _*)(completion => {
            completion.submissionId == submissionId
          })
          .map {
            case Some(response) =>
              Status.fromCodeValue(response.completion.getStatus.code).getCode
            case None =>
              fail(s"Did not find completion for request with submission id $submissionId")
          }
    }
}
