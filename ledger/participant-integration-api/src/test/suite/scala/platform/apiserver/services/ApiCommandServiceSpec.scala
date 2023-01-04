// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.daml.grpc.RpcProtoExtractors
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.ledger.client.services.commands.tracker.CompletionResponse
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.ApiCommandServiceSpec._
import com.daml.platform.apiserver.services.tracking.Tracker
import com.google.rpc.Code
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Deadline, Status}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class ApiCommandServiceSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {
  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

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
      val submissionTracker = mock[Tracker]
      when(
        submissionTracker.track(any[CommandSubmission])(
          any[ExecutionContext],
          any[LoggingContext],
        )
      ).thenReturn(
        Future.successful(
          Right(completionSuccess)
        )
      )

      openChannel(
        new ApiCommandService(
          UnimplementedTransactionServices,
          submissionTracker,
        )
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub.submitAndWaitForTransactionId(request).map { response =>
          response.transactionId should be("transaction ID")
          response.completionOffset shouldBe "offset"
          verify(submissionTracker).track(
            eqTo(CommandSubmission(commands))
          )(any[ExecutionContext], any[LoggingContext])
          succeed
        }
      }
    }

    "pass the provided deadline to the tracker as a timeout" in {
      val now = Instant.parse("2021-09-01T12:00:00Z")
      val deadlineTicker = new Deadline.Ticker {
        override def nanoTime(): Long =
          now.getEpochSecond * TimeUnit.SECONDS.toNanos(1) + now.getNano
      }

      val commands = someCommands()
      val submissionTracker = mock[Tracker]
      when(
        submissionTracker.track(any[CommandSubmission])(
          any[ExecutionContext],
          any[LoggingContext],
        )
      ).thenReturn(
        Future.successful(
          Right(completionSuccess)
        )
      )

      openChannel(
        new ApiCommandService(
          UnimplementedTransactionServices,
          submissionTracker,
        ),
        deadlineTicker,
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub
          .withDeadline(Deadline.after(30, TimeUnit.SECONDS, deadlineTicker))
          .submitAndWaitForTransactionId(request)
          .map { response =>
            response.transactionId should be("transaction ID")
            verify(submissionTracker).track(
              eqTo(CommandSubmission(commands, timeout = Some(Duration.ofSeconds(30))))
            )(any[ExecutionContext], any[LoggingContext])
            succeed
          }
      }
    }

    "time out if the tracker times out" in {
      val commands = someCommands()
      val submissionTracker = mock[Tracker]
      when(
        submissionTracker.track(any[CommandSubmission])(
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

      openChannel(
        new ApiCommandService(
          UnimplementedTransactionServices,
          submissionTracker,
        )
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub.submitAndWaitForTransactionId(request).failed.map {
          case RpcProtoExtractors.Exception(RpcProtoExtractors.Status(Code.DEADLINE_EXCEEDED)) =>
            succeed
          case unexpected => fail(s"Unexpected exception", unexpected)
        }
      }
    }

    "close the supplied tracker when closed" in {
      val submissionTracker = mock[Tracker]
      val service = new ApiCommandService(
        UnimplementedTransactionServices,
        submissionTracker,
      )

      verifyZeroInteractions(submissionTracker)

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
    ledgerId = "ledger ID",
    commandId = "command ID",
    commands = Seq(
      Command.of(Command.Command.Create(CreateCommand()))
    ),
  )

  private def openChannel(
      service: ApiCommandService,
      deadlineTicker: Deadline.Ticker = Deadline.getSystemTicker,
  ): ResourceOwner[CommandServiceGrpc.CommandServiceStub] =
    for {
      name <- ResourceOwner.forValue(() => UUID.randomUUID().toString)
      _ <- ResourceOwner.forServer(
        InProcessServerBuilder
          .forName(name)
          .deadlineTicker(deadlineTicker)
          .addService(() => CommandService.bindService(service, ExecutionContext.global)),
        shutdownTimeout = 10.seconds,
      )
      channel <- ResourceOwner.forChannel(
        InProcessChannelBuilder.forName(name),
        shutdownTimeout = 10.seconds,
      )
    } yield CommandServiceGrpc.stub(channel)
}
