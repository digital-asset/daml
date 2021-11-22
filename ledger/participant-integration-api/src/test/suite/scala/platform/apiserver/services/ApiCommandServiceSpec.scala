// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.Duration

import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.grpc.RpcProtoExtractors
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.{ApplicationId, CommandId, Commands, LedgerId}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.services.commands.tracker.CompletionResponse
import com.daml.lf.command
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.ApiCommandServiceSpec._
import com.daml.platform.apiserver.services.tracking.{InternalCommandSubmission, Tracker}
import com.google.rpc.Code
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class ApiCommandServiceSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val errorCodesVersionSwitcher = mock[ErrorCodesVersionSwitcher]

  s"the command service" should {
    val completionSuccess = CompletionResponse.CompletionSuccess(
      Completion(
        commandId = "command ID",
        status = Some(OkStatus),
        transactionId = "transaction ID",
      ),
      Some(
        Checkpoint(offset = Some(LedgerOffset(LedgerOffset.Value.Absolute("offset"))))
      ),
    )
    "submit a request, and wait for a response" in {
      val commands = someCommands()
      val submissionTracker = mock[Tracker[InternalCommandSubmission]]
      when(
        submissionTracker.track(any[InternalCommandSubmission])(
          any[ExecutionContext],
          any[LoggingContext],
        )
      ).thenReturn(
        Future.successful(
          Right(completionSuccess)
        )
      )

      val service = new ApiCommandService(
        UnimplementedTransactionServices,
        submissionTracker,
        errorCodesVersionSwitcher,
      )
      val request = SubmitRequest(commands, "")
      service.submitAndWait(request, None).map { response =>
        response.transactionId should be("transaction ID")
        response.completionOffset shouldBe "offset"
        verify(submissionTracker).track(
          eqTo(InternalCommandSubmission(request))
        )(any[ExecutionContext], any[LoggingContext])
        verifyZeroInteractions(errorCodesVersionSwitcher)
        succeed
      }
    }

    "time out if the tracker times out" in {
      def testTrackerTimeout(switcher: ErrorCodesVersionSwitcher, expectedStatusCode: Code) = {
        val commands = someCommands()
        val submissionTracker = mock[Tracker[InternalCommandSubmission]]
        when(
          submissionTracker.track(any[InternalCommandSubmission])(
            any[ExecutionContext],
            any[LoggingContext],
          )
        ).thenReturn(
          Future.successful(
            Left(
              CompletionResponse.QueueCompletionFailure(
                CompletionResponse.TimeoutResponse("command ID")
              )
            )
          )
        )
        val service =
          new ApiCommandService(
            UnimplementedTransactionServices,
            submissionTracker,
            switcher,
          )
        val request = SubmitRequest(commands, "")
        service.submitAndWaitForTransaction(request, None).failed.map {
          case RpcProtoExtractors.Exception(RpcProtoExtractors.Status(`expectedStatusCode`)) =>
            succeed
          case unexpected => fail(s"Unexpected exception", unexpected)
        }
      }

      testTrackerTimeout(
        new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = false),
        Code.ABORTED,
      )
      testTrackerTimeout(
        new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = true),
        Code.DEADLINE_EXCEEDED,
      )
    }

    "close the supplied tracker when closed" in {
      val submissionTracker = mock[Tracker[InternalCommandSubmission]]
      val service = new ApiCommandService(
        UnimplementedTransactionServices,
        submissionTracker,
        errorCodesVersionSwitcher,
      )

      verifyZeroInteractions(submissionTracker)
      verifyZeroInteractions(errorCodesVersionSwitcher)

      service.close()
      verify(submissionTracker).close()
      succeed
    }
  }
}

object ApiCommandServiceSpec {
  private val UnimplementedTransactionServices = new ApiCommandService.TransactionServices(
    getTransactionById = _ => Future.failed(new RuntimeException("This should never be called.")),
    getFlatTransactionById = _ =>
      Future.failed(new RuntimeException("This should never be called.")),
  )

  private val OkStatus = StatusProto.of(Status.Code.OK.value, "", Seq.empty)

  private def someCommands() = Commands(
    ledgerId = LedgerId("ledger ID"),
    commandId = CommandId(Ref.CommandId.assertFromString("command ID")),
    workflowId = None,
    applicationId = ApplicationId(Ref.ApplicationId.assertFromString("app ID")),
    submissionId = None,
    actAs = Set.empty,
    readAs = Set.empty,
    submittedAt = Timestamp.now(),
    deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(1)),
    commands = command.Commands(
      ImmArray.empty,
      Timestamp.now(),
      "",
    ),
  )

}
