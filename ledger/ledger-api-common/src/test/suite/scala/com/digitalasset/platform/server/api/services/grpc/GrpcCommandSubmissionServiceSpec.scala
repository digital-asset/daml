// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import java.time.{Duration, Instant}

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.testing.utils.MockMessages.{
  applicationId,
  commandId,
  commands,
  ledgerId,
  party,
  submitRequest,
  workflowId,
}
import com.daml.ledger.api.v1.commands.{Command, CreateCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.lf.data.Ref
import com.daml.metrics.Metrics
import com.daml.platform.server.api.services.domain.CommandSubmissionService
import com.daml.telemetry.{SpanAttribute, TelemetryContext, TelemetrySpecBase}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class GrpcCommandSubmissionServiceSpec
    extends AsyncWordSpec
    with TelemetrySpecBase
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {

  import GrpcCommandSubmissionServiceSpec._

  "GrpcCommandSubmissionService" should {
    "propagate trace context" in {
      val mockCommandSubmissionService = mock[CommandSubmissionService with AutoCloseable]
      when(mockCommandSubmissionService.submit(any[SubmitRequest])(any[TelemetryContext]))
        .thenReturn(Future.unit)
      val grpcCommandSubmissionService = new GrpcCommandSubmissionService(
        mockCommandSubmissionService,
        ledgerId = LedgerId(ledgerId),
        currentLedgerTime = () => Instant.EPOCH,
        currentUtcTime = () => Instant.EPOCH,
        maxDeduplicationTime = () => Some(Duration.ZERO),
        maxSkew = () => Some(Duration.ZERO),
        submissionIdGenerator = () => Ref.SubmissionId.assertFromString("submissionId"),
        metrics = new Metrics(new MetricRegistry),
      )

      val span = anEmptySpan()
      val scope = span.makeCurrent()
      try {
        grpcCommandSubmissionService
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
  }
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
