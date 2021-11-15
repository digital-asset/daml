// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.error.definitions.LedgerApiErrors
import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.admin.config_management_service.{
  SetTimeModelRequest,
  SetTimeModelResponse,
  TimeModel,
}
import com.google.protobuf.duration.Duration
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final class ConfigManagementServiceIT extends LedgerTestSuite {
  test(
    "CMSetAndGetTimeModel",
    "It should be able to get, set and restore the time model",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val newTimeModel = TimeModel(
      avgTransactionLatency = Some(Duration(0, 1)),
      minSkew = Some(Duration(60, 0)),
      maxSkew = Some(Duration(120, 0)),
    )
    for {
      // Get the current time model
      response1 <- ledger.getTimeModel()
      oldTimeModel = {
        assert(response1.timeModel.isDefined, "Expected time model to be defined")
        response1.timeModel.get
      }

      // Set a new temporary time model
      t1 <- ledger.time()
      _ <- ledger.setTimeModel(
        mrt = t1.plusSeconds(30),
        generation = response1.configurationGeneration,
        newTimeModel = newTimeModel,
      )

      // Retrieve the new model
      response2 <- ledger.getTimeModel()

      // Restore the original time model
      t2 <- ledger.time()
      _ <- ledger.setTimeModel(
        mrt = t2.plusSeconds(30),
        generation = response2.configurationGeneration,
        newTimeModel = oldTimeModel,
      )

      // Verify that we've restored the original time model
      response3 <- ledger.getTimeModel()

      // Try to set a time model with an expired MRT.
      t3 <- ledger.time()
      expiredMRTFailure <- ledger
        .setTimeModel(
          mrt = t3.minusSeconds(10),
          generation = response3.configurationGeneration,
          newTimeModel = oldTimeModel,
        )
        .mustFail("setting a time model with an expired MRT")

      // Above operation finished on a timeout, but may still succeed asynchronously.
      // Stabilize the ledger state before leaving the test case.
      _ <- stabilize(ledger)
    } yield {
      assert(
        response1.configurationGeneration < response2.configurationGeneration,
        "Expected configuration generation to have increased after setting time model",
      )
      assert(
        response2.configurationGeneration < response3.configurationGeneration,
        "Expected configuration generation to have increased after setting time model the second time",
      )
      assert(response2.timeModel.contains(newTimeModel), "Setting the new time model failed")
      assert(
        response3.timeModel.equals(response1.timeModel),
        "Restoring the original time model failed",
      )

      assertGrpcError(
        ledger,
        expiredMRTFailure,
        Status.Code.ABORTED,
        LedgerApiErrors.AdminServices.RequestTimeOut,
        exceptionMessageSubstring = None,
      )
    }
  })

  // TODO(JM): Test that sets the time model and verifies that a transaction with invalid
  // ttl/mrt won't be accepted. Can only implement once ApiSubmissionService properly
  // uses currently set configuration.

  test(
    "CMSetConflicting",
    "Conflicting generation should be rejected",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      // Get the current time model
      response1 <- ledger.getTimeModel()
      oldTimeModel = {
        assert(response1.timeModel.isDefined, "Expected time model to be defined")
        response1.timeModel.get
      }

      // Set a new time model with the next generation
      t1 <- ledger.time()
      _ <- ledger.setTimeModel(
        mrt = t1.plusSeconds(30),
        generation = response1.configurationGeneration,
        newTimeModel = oldTimeModel,
      )

      // Set a new time model with the same generation
      t2 <- ledger.time()
      failure <- ledger
        .setTimeModel(
          mrt = t2.plusSeconds(30),
          generation = response1.configurationGeneration,
          newTimeModel = oldTimeModel,
        )
        .mustFail("setting Time Model with an outdated generation")
    } yield {
      assertGrpcError(
        ledger,
        failure,
        Status.Code.INVALID_ARGUMENT,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = None,
      )
    }
  })

  test(
    "CMConcurrentSetConflicting",
    "Two concurrent conflicting generation should be rejected/accepted",
    allocate(NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      // Get the current time model
      response1 <- ledger.getTimeModel()
      oldTimeModel = {
        assert(response1.timeModel.isDefined, "Expected time model to be defined")
        response1.timeModel.get
      }

      // Set a new time model with the next generation in parallel
      t1 <- ledger.time()
      f1 = ledger.setTimeModel(
        mrt = t1.plusSeconds(30),
        generation = response1.configurationGeneration,
        newTimeModel = oldTimeModel,
      )
      f2 = ledger.setTimeModel(
        mrt = t1.plusSeconds(30),
        generation = response1.configurationGeneration,
        newTimeModel = oldTimeModel,
      )

      failure <- f1
        .flatMap(_ => f2)
        .mustFail("setting Time Model with an outdated generation")

      // Check if generation got updated (meaning, one of the above succeeded)
      response2 <- ledger.getTimeModel()
    } yield {
      assert(
        response1.configurationGeneration + 1 == response2.configurationGeneration,
        s"New configuration's generation (${response2.configurationGeneration} should be original configurations's generation (${response1.configurationGeneration} + 1) )",
      )
      Try {
        // if the "looser" command fails already on command submission (the winner completed before looser submission is over)
        assertGrpcError(
          ledger,
          failure,
          Status.Code.INVALID_ARGUMENT,
          LedgerApiErrors.RequestValidation.InvalidArgument,
          Some("Mismatching configuration generation"),
        )
      }.recover { case _ =>
        // if the "looser" command fails after command submission (the winner completed after looser did submit the configuration change)
        assertGrpcError(
          ledger,
          failure,
          Status.Code.ABORTED,
          LedgerApiErrors.AdminServices.ConfigurationEntryRejected,
          Some("Generation mismatch"),
        )
      } match {
        case Failure(GrpcException(GrpcStatus(notExpectedCode, _), _)) =>
          fail(s"One of the submissions failed with an unexpected status code: $notExpectedCode")

        case Failure(notExpectedException) =>
          fail(
            s"Unexpected exception: type:${notExpectedException.getClass.getName}, message:${notExpectedException.getMessage}"
          )
        case Success(_) => ()
      }
    }
  })

  test(
    "CMDuplicateSubmissionId",
    "Duplicate submission ids are accepted when config changed twice",
    allocate(NoParties, NoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(alpha), Participant(beta)) =>
    // Multiple config changes should never cause duplicates. Participant adds extra entropy to the
    // submission id to ensure client does not inadvertently cause problems by poor selection
    // of submission ids.
    for {
      (req1, req2) <- generateRequest(alpha).map(r =>
        r -> r
          .update(_.configurationGeneration := r.configurationGeneration + 1)
      )
      _ <- ignoreNotAuthorized(alpha.setTimeModel(req1))
      _ <- synchronize(alpha, beta)
      _ <- ignoreNotAuthorized(beta.setTimeModel(req2))
    } yield ()

  })

  def generateRequest(
      participant: ParticipantTestContext
  )(implicit ec: ExecutionContext): Future[SetTimeModelRequest] =
    for {
      response <- participant.getTimeModel()
      oldTimeModel = response.timeModel.getOrElse(
        throw new AssertionError("Expected time model to be defined")
      )
      t1 <- participant.time()
      req = participant.setTimeModelRequest(
        mrt = t1.plusSeconds(30),
        generation = response.configurationGeneration,
        newTimeModel = oldTimeModel,
      )
    } yield req

  private val notAuthorizedPattern = "not authorized".r.pattern

  // On some ledger implementations only one participant is allowed to modify config, others will
  // fail with an authorization failure.
  def ignoreNotAuthorized(
      call: Future[SetTimeModelResponse]
  )(implicit executionContext: ExecutionContext): Future[Option[SetTimeModelResponse]] =
    call.transform {
      case Success(value: SetTimeModelResponse) =>
        Success(Some(value))
      case Failure(GrpcException(GrpcStatus(Status.Code.ABORTED, Some(msg)), _))
          if notAuthorizedPattern.matcher(msg).find() =>
        Success(None)
      case Failure(failure) =>
        Failure(failure)

    }

  // Stabilize the ledger by writing a new element and observing it in the indexDb.
  // The allocateParty method fits the bill.
  private def stabilize(ledger: ParticipantTestContext)(implicit
      executionContext: ExecutionContext
  ): Future[Unit] =
    ledger.allocateParty().map(_ => ())
}
