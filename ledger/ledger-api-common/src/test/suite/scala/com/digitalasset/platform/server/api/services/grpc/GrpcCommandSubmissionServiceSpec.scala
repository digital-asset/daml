// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import java.time.{Duration, Instant}

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.testing.utils.MockMessages._
import com.daml.ledger.api.v1.commands.{Command, CreateCommand, DisclosedContract}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.server.api.services.domain.CommandSubmissionService
import com.daml.telemetry.{SpanAttribute, TelemetryContext, TelemetrySpecBase}
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.util.{Failure, Success}

class GrpcCommandSubmissionServiceSpec
    extends AsyncWordSpec
    with TelemetrySpecBase
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val generatedSubmissionId = "generated-submission-id"

  import GrpcCommandSubmissionServiceSpec._
  "GrpcCommandSubmissionService" should {
    "propagate trace context" in {
      val span = anEmptySpan()
      val scope = span.makeCurrent()
      val mockCommandSubmissionService = mock[CommandSubmissionService with AutoCloseable]
      when(mockCommandSubmissionService.submit(any[SubmitRequest])(any[TelemetryContext]))
        .thenReturn(Future.unit)

      try {
        grpcCommandSubmissionService(mockCommandSubmissionService)
          .submit(aSubmitRequest)
          .map { _ =>
            val spanAttributes = spanExporter.finishedSpanAttributes
            spanAttributes should contain(SpanAttribute.ApplicationId -> applicationId)
            spanAttributes should contain(SpanAttribute.CommandId -> commandId)
            spanAttributes should contain(SpanAttribute.Submitter -> party)
            spanAttributes should contain(SpanAttribute.WorkflowId -> workflowId)
          }
      } finally {
        scope.close()
        span.end()
      }
    }

    "propagate submission id" in {
      val expectedSubmissionId = "explicitSubmissionId"
      val requestWithSubmissionId =
        aSubmitRequest.update(_.commands.submissionId := expectedSubmissionId)
      val requestCaptor = ArgCaptor[com.daml.ledger.api.messages.command.submission.SubmitRequest]
      val mockCommandSubmissionService = mock[CommandSubmissionService with AutoCloseable]
      when(mockCommandSubmissionService.submit(any[SubmitRequest])(any[TelemetryContext]))
        .thenReturn(Future.unit)

      grpcCommandSubmissionService(mockCommandSubmissionService)
        .submit(requestWithSubmissionId)
        .map { _ =>
          verify(mockCommandSubmissionService).submit(requestCaptor.capture)(any[TelemetryContext])
          requestCaptor.value.commands.submissionId shouldBe Some(expectedSubmissionId)
        }
    }

    "set submission id if empty" in {
      val requestCaptor = ArgCaptor[com.daml.ledger.api.messages.command.submission.SubmitRequest]

      val mockCommandSubmissionService = mock[CommandSubmissionService with AutoCloseable]
      when(mockCommandSubmissionService.submit(any[SubmitRequest])(any[TelemetryContext]))
        .thenReturn(Future.unit)

      grpcCommandSubmissionService(mockCommandSubmissionService)
        .submit(aSubmitRequest)
        .map { _ =>
          verify(mockCommandSubmissionService).submit(requestCaptor.capture)(any[TelemetryContext])
          requestCaptor.value.commands.submissionId shouldBe Some(generatedSubmissionId)
        }
    }

    "reject submission on explicit disclosure disabled with provided disclosed contracts" in {
      val mockCommandSubmissionService = mock[CommandSubmissionService with AutoCloseable]

      val submissionWithDisclosedContracts =
        aSubmitRequest.update(_.commands.disclosedContracts.set(Seq(DisclosedContract())))

      grpcCommandSubmissionService(mockCommandSubmissionService)
        .submit(submissionWithDisclosedContracts)
        .map { _ =>
          verifyZeroInteractions(mockCommandSubmissionService)
          succeed
        }
        .transform {
          case Failure(exception)
              if exception.getMessage.contains(
                "feature in development: disclosed_contracts should not be set"
              ) =>
            Success(succeed)
          case other => fail(s"Unexpected result: $other")
        }
    }
  }

  private def grpcCommandSubmissionService(
      commandSubmissionService: CommandSubmissionService with AutoCloseable
  ) =
    new GrpcCommandSubmissionService(
      commandSubmissionService,
      ledgerId = LedgerId(ledgerId),
      currentLedgerTime = () => Instant.EPOCH,
      currentUtcTime = () => Instant.EPOCH,
      maxDeduplicationDuration = () => Some(Duration.ZERO),
      submissionIdGenerator = () => Ref.SubmissionId.assertFromString(generatedSubmissionId),
      metrics = Metrics.ForTesting,
      explicitDisclosureUnsafeEnabled = false,
    )
}

object GrpcCommandSubmissionServiceSpec {
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

  private val aSubmitRequest = submitRequest.copy(
    commands = Some(commands.copy(commands = Seq(aCommand)))
  )
}
