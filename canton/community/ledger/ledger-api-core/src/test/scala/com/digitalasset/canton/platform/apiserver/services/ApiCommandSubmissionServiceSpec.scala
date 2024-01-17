// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v1.commands.{Command, CreateCommand, DisclosedContract}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.lf.data.Ref
import com.daml.tracing.{DefaultOpenTelemetry, SpanAttribute}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.MockMessages.*
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.canton.ledger.api.services.CommandSubmissionService
import com.digitalasset.canton.ledger.api.validation.{CommandsValidator, ValidateDisclosedContracts}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.tracing.TestTelemetrySetup
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}
import scala.concurrent.Future
import scala.util.{Failure, Success}

class ApiCommandSubmissionServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with BaseTest
    with BeforeAndAfterEach {
  private val generatedSubmissionId = "generated-submission-id"

  var testTelemetrySetup: TestTelemetrySetup = _
  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
  }
  override def afterEach(): Unit = {
    testTelemetrySetup.close()
  }

  import ApiCommandSubmissionServiceSpec.*
  "ApiCommandSubmissionService" should {
    "propagate trace context" in {
      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()
      val mockCommandSubmissionService = mock[CommandSubmissionService & AutoCloseable]
      when(
        mockCommandSubmissionService
          .submit(any[SubmitRequest])(any[LoggingContextWithTrace])
      )
        .thenReturn(Future.unit)

      try {
        grpcCommandSubmissionService(mockCommandSubmissionService)
          .submit(aSubmitRequest)
          .map { _ =>
            val spanAttributes = testTelemetrySetup.reportedSpanAttributes
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
      val requestCaptor =
        ArgCaptor[SubmitRequest]
      val mockCommandSubmissionService = mock[CommandSubmissionService & AutoCloseable]
      when(
        mockCommandSubmissionService
          .submit(any[SubmitRequest])(any[LoggingContextWithTrace])
      )
        .thenReturn(Future.unit)

      grpcCommandSubmissionService(mockCommandSubmissionService)
        .submit(requestWithSubmissionId)
        .map { _ =>
          verify(mockCommandSubmissionService)
            .submit(requestCaptor.capture)(any[LoggingContextWithTrace])
          requestCaptor.value.commands.submissionId shouldBe Some(expectedSubmissionId)
        }
    }

    "set submission id if empty" in {
      val requestCaptor =
        ArgCaptor[SubmitRequest]

      val mockCommandSubmissionService = mock[CommandSubmissionService & AutoCloseable]
      when(
        mockCommandSubmissionService
          .submit(any[SubmitRequest])(any[LoggingContextWithTrace])
      )
        .thenReturn(Future.unit)

      grpcCommandSubmissionService(mockCommandSubmissionService)
        .submit(aSubmitRequest)
        .map { _ =>
          verify(mockCommandSubmissionService)
            .submit(requestCaptor.capture)(any[LoggingContextWithTrace])
          requestCaptor.value.commands.submissionId shouldBe Some(generatedSubmissionId)
        }
    }

    "reject submission on explicit disclosure disabled with provided disclosed contracts" in {
      val mockCommandSubmissionService = mock[CommandSubmissionService & AutoCloseable]

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
                "feature disabled: disclosed_contracts should not be set"
              ) =>
            Success(succeed)
          case other => fail(s"Unexpected result: $other")
        }
    }
  }

  private def grpcCommandSubmissionService(
      commandSubmissionService: CommandSubmissionService & AutoCloseable
  ) =
    new ApiCommandSubmissionService(
      commandSubmissionService,
      currentLedgerTime = () => Instant.EPOCH,
      currentUtcTime = () => Instant.EPOCH,
      maxDeduplicationDuration = () => Some(Duration.ZERO),
      submissionIdGenerator = () => Ref.SubmissionId.assertFromString(generatedSubmissionId),
      metrics = Metrics.ForTesting,
      telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
      loggerFactory = loggerFactory,
      commandsValidator = new CommandsValidator(
        ledgerId = LedgerId(ledgerId),
        resolveToTemplateId = _ => fail("should not be called"),
        upgradingEnabled = false,
        validateDisclosedContracts = new ValidateDisclosedContracts(false),
      ),
    )
}

object ApiCommandSubmissionServiceSpec {
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

  private val aSubmitRequest = submitRequestV1.copy(
    commands = Some(commandsV1.copy(commands = Seq(aCommand)))
  )
}
