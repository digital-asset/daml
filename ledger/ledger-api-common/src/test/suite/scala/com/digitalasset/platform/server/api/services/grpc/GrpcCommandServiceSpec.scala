// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.MockMessages._
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v1.commands.{Command, CreateCommand, DisclosedContract}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.google.protobuf.empty.Empty
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.util.{Failure, Success}

class GrpcCommandServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {

  import GrpcCommandServiceSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "GrpcCommandService" should {
    "generate a submission ID if it's empty" in {
      val submissionCounter = new AtomicInteger

      val mockCommandService = mock[CommandService with AutoCloseable]
      when(mockCommandService.submitAndWait(any[SubmitAndWaitRequest]))
        .thenReturn(Future.successful(Empty.defaultInstance))
      when(mockCommandService.submitAndWaitForTransaction(any[SubmitAndWaitRequest]))
        .thenReturn(Future.successful(SubmitAndWaitForTransactionResponse.defaultInstance))
      when(mockCommandService.submitAndWaitForTransactionId(any[SubmitAndWaitRequest]))
        .thenReturn(Future.successful(SubmitAndWaitForTransactionIdResponse.defaultInstance))
      when(mockCommandService.submitAndWaitForTransactionTree(any[SubmitAndWaitRequest]))
        .thenReturn(Future.successful(SubmitAndWaitForTransactionTreeResponse.defaultInstance))

      val grpcCommandService = new GrpcCommandService(
        mockCommandService,
        ledgerId = LedgerId(ledgerId),
        currentLedgerTime = () => Instant.EPOCH,
        currentUtcTime = () => Instant.EPOCH,
        maxDeduplicationDuration = () => Some(Duration.ZERO),
        generateSubmissionId = () =>
          Ref.SubmissionId.assertFromString(
            s"$submissionIdPrefix${submissionCounter.incrementAndGet()}"
          ),
        explicitDisclosureUnsafeEnabled = false,
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
        def expectedSubmitAndWaitRequest(submissionIdSuffix: String) =
          aSubmitAndWaitRequestWithNoSubmissionId.copy(commands =
            aSubmitAndWaitRequestWithNoSubmissionId.commands
              .map(_.copy(submissionId = s"$submissionIdPrefix$submissionIdSuffix"))
          )
        verify(mockCommandService).submitAndWait(expectedSubmitAndWaitRequest("1"))
        verify(mockCommandService).submitAndWaitForTransaction(expectedSubmitAndWaitRequest("2"))
        verify(mockCommandService).submitAndWaitForTransactionId(expectedSubmitAndWaitRequest("3"))
        verify(mockCommandService).submitAndWaitForTransactionTree(
          expectedSubmitAndWaitRequest("4")
        )
        succeed
      }
    }
    "reject submission on explicit disclosure disabled with provided disclosed contracts" in {
      val mockCommandService = mock[CommandService with AutoCloseable]

      val grpcCommandService = new GrpcCommandService(
        mockCommandService,
        ledgerId = LedgerId(ledgerId),
        currentLedgerTime = () => Instant.EPOCH,
        currentUtcTime = () => Instant.EPOCH,
        maxDeduplicationDuration = () => Some(Duration.ZERO),
        generateSubmissionId = () => Ref.SubmissionId.assertFromString(s"submissionId"),
        explicitDisclosureUnsafeEnabled = false,
      )

      val submissionWithDisclosedContracts = aSubmitAndWaitRequestWithNoSubmissionId.update(
        _.commands.disclosedContracts.set(Seq(DisclosedContract()))
      )

      def expectFailedOnProvidedDisclosedContracts(f: Future[_]): Future[Assertion] = f.transform {
        case Failure(exception)
            if exception.getMessage.contains(
              "feature in development: disclosed_contracts should not be set"
            ) =>
          Success(succeed)
        case other => fail(s"Unexpected result: $other")
      }

      for {
        _ <- expectFailedOnProvidedDisclosedContracts(
          grpcCommandService.submitAndWait(submissionWithDisclosedContracts)
        )
        _ <- expectFailedOnProvidedDisclosedContracts(
          grpcCommandService.submitAndWaitForTransaction(submissionWithDisclosedContracts)
        )
        _ <- expectFailedOnProvidedDisclosedContracts(
          grpcCommandService.submitAndWaitForTransactionId(submissionWithDisclosedContracts)
        )
        _ <- expectFailedOnProvidedDisclosedContracts(
          grpcCommandService.submitAndWaitForTransactionTree(submissionWithDisclosedContracts)
        )
      } yield {
        verifyZeroInteractions(mockCommandService)
        succeed
      }
    }

  }
}

object GrpcCommandServiceSpec {
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

  private val submissionIdPrefix = "submissionId-"
}
