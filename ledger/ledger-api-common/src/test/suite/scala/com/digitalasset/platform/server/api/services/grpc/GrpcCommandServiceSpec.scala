// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.{ApplicationId, CommandId, Commands, LedgerId, SubmissionId}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.testing.utils.MockMessages._
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v1.commands.{Command, CreateCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.api.validation.SubmitAndWaitRequestValidator
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.lf.command
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.daml.lf.value.Value.ValueList
import com.daml.logging.LoggingContext
import com.daml.platform.server.api.services.domain
import io.grpc.Deadline
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {

  import GrpcCommandServiceSpec._

  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  val internalSubmitRequest: SubmitRequest = aSubmitRequest(None)

  "GrpcCommandService" should {
    "generate a submission ID if it's empty" in {
      val submissionCounter = new AtomicInteger

      val (mockCommandService, requestValidator, grpcCommandService) = buildGrpcService(
        Ref.SubmissionId.assertFromString(
          s"$submissionIdPrefix${submissionCounter.incrementAndGet()}"
        )
      )

      when(
        requestValidator.validate(
          any[SubmitAndWaitRequest],
          any[Instant],
          any[Instant],
          any[Option[Duration]],
        )(
          any[ContextualizedErrorLogger]
        )
      ).thenAnswer((request: SubmitAndWaitRequest) =>
        Right(aSubmitRequest(request.commands.map(_.submissionId)))
      )

      for {
        _ <- grpcCommandService.submitAndWait(aSubmitAndWaitRequestWithNoSubmissionId)
        _ <- grpcCommandService.submitAndWaitForTransaction(aSubmitAndWaitRequestWithNoSubmissionId)
        _ <- grpcCommandService.submitAndWaitForTransactionId(
          aSubmitAndWaitRequestWithNoSubmissionId
        )
        _ <- grpcCommandService.submitAndWaitForTransactionTree(
          aSubmitAndWaitRequestWithNoSubmissionId
        )
      } yield {
        def expectedSubmitAndWaitRequest(submissionIdSuffix: String) = {
          aSubmitRequest(Some(s"$submissionIdPrefix$submissionIdSuffix"))
        }
        val mock = inOrder(mockCommandService)

        mock.verify(mockCommandService).submitAndWait(expectedSubmitAndWaitRequest("1"), None)
        mock
          .verify(mockCommandService)
          .submitAndWaitForTransaction(expectedSubmitAndWaitRequest("2"), None)
        mock.verify(mockCommandService).submitAndWait(expectedSubmitAndWaitRequest("3"), None)
        mock
          .verify(mockCommandService)
          .submitAndWaitForTransactionTree(
            expectedSubmitAndWaitRequest("4"),
            None,
          )
        succeed
      }
    }

    "pass the provided deadline to the tracker as a timeout" in {
      val now = Instant.parse("2021-09-01T12:00:00Z")
      val deadlineTicker = new Deadline.Ticker {
        override def nanoTime(): Long =
          now.getEpochSecond * TimeUnit.SECONDS.toNanos(1) + now.getNano
      }
      val (mockCommandService, requestValidator, grpcCommandService) = buildGrpcService()
      when(
        requestValidator.validate(
          any[SubmitAndWaitRequest],
          any[Instant],
          any[Instant],
          any[Option[Duration]],
        )(
          any[ContextualizedErrorLogger]
        )
      ).thenReturn(
        Right(internalSubmitRequest)
      )
      openChannel(
        grpcCommandService,
        deadlineTicker,
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub
          .withDeadline(Deadline.after(30, TimeUnit.SECONDS, deadlineTicker))
          .submitAndWaitForTransactionId(request)
          .map { response =>
            response.transactionId should be("transaction ID")
            verify(mockCommandService).submitAndWait(
              internalSubmitRequest,
              Some(Duration.ofSeconds(30)),
            )
            succeed
          }
      }
    }
  }

  private def initializeMockCommandService = {

    val mockCommandService = mock[domain.CommandService with AutoCloseable]
    when(mockCommandService.submitAndWait(any[SubmitRequest], any[Option[Duration]]))
      .thenReturn(Future.successful(SubmitAndWaitForTransactionIdResponse.defaultInstance))
    when(mockCommandService.submitAndWaitForTransaction(any[SubmitRequest], any[Option[Duration]]))
      .thenReturn(Future.successful(SubmitAndWaitForTransactionResponse.defaultInstance))
    when(
      mockCommandService.submitAndWaitForTransactionTree(any[SubmitRequest], any[Option[Duration]])
    )
      .thenReturn(Future.successful(SubmitAndWaitForTransactionTreeResponse.defaultInstance))
    mockCommandService
  }

  private def buildGrpcService(
      submissionIdGenerator: => Ref.SubmissionId = Ref.SubmissionId.assertFromString(
        s"submissionid"
      )
  ) = {
    val mockCommandService = initializeMockCommandService
    val requestValidator = mock[SubmitAndWaitRequestValidator]
    (
      mockCommandService,
      requestValidator,
      new GrpcCommandService(
        mockCommandService,
        ledgerId = LedgerId(ledgerId),
        currentLedgerTime = () => Instant.EPOCH,
        currentUtcTime = () => Instant.EPOCH,
        maxDeduplicationTime = () => Some(Duration.ZERO),
        generateSubmissionId = () => submissionIdGenerator,
        validator = requestValidator,
      ),
    )
  }
}

object GrpcCommandServiceSpec {
  private def openChannel(
      service: GrpcCommandService,
      deadlineTicker: Deadline.Ticker,
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

  private val aCommand = Command.of(
    Command.Command.Create(
      CreateCommand(
        Some(Identifier("package", moduleName = "module", entityName = "entity")),
        Some(
          Record(
            Some(Identifier("package", moduleName = "module", entityName = "entity")),
            Seq(RecordField("something", Some(Value(Value.Sum.Bool(true))))),
          )
        ),
      )
    )
  )

  private val aSubmitAndWaitRequestWithNoSubmissionId = submitAndWaitRequest.copy(
    commands = Some(commands.copy(commands = Seq(aCommand), submissionId = ""))
  )
  private def aSubmitRequest(submissionId: Option[String]) = SubmitRequest(
    Commands(
      LedgerId(commands.ledgerId),
      Some(api.domain.WorkflowId(Ref.WorkflowId.assertFromString(commands.workflowId))),
      ApplicationId(Ref.ApplicationId.assertFromString(commands.applicationId)),
      CommandId(Ref.CommandId.assertFromString(commands.commandId)),
      submissionId.map(submissionId =>
        SubmissionId(Ref.SubmissionId.assertFromString(submissionId))
      ),
      commands.actAs.toSet.map(Ref.Party.assertFromString),
      commands.readAs.toSet.map(Ref.Party.assertFromString),
      Time.Timestamp.Epoch,
      DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      command.Commands(
        ImmArray(
          command.CreateCommand(
            Ref.Identifier(
              Ref.PackageId.assertFromString("package"),
              Ref.QualifiedName.assertFromString("module:entity"),
            ),
            ValueList(FrontStack.empty),
          )
        ),
        Time.Timestamp.Epoch,
        "",
      ),
    ),
    "",
  )

  private val submissionIdPrefix = "submissionId-"
}
