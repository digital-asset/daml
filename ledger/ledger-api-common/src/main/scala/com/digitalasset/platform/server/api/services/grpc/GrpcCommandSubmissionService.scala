// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.{
  CommandSubmissionService => ApiCommandSubmissionService
}
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest => ApiSubmitRequest,
}
import com.daml.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.services.domain.CommandSubmissionService
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import com.daml.telemetry.{DefaultTelemetry, SpanAttribute, TelemetryContext}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandSubmissionService(
    override protected val service: CommandSubmissionService with AutoCloseable,
    ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: () => Option[Duration],
    submissionIdGenerator: SubmissionIdGenerator,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends ApiCommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private val validator = new SubmitRequestValidator(
    new CommandsValidator(ledgerId)
  )

  override def submit(request: ApiSubmitRequest): Future[Empty] = {
    implicit val telemetryContext: TelemetryContext =
      DefaultTelemetry.contextFromGrpcThreadLocalContext()
    request.commands.foreach { commands =>
      telemetryContext.setAttribute(SpanAttribute.ApplicationId, commands.applicationId)
      telemetryContext.setAttribute(SpanAttribute.CommandId, commands.commandId)
      telemetryContext.setAttribute(SpanAttribute.Submitter, commands.party)
      telemetryContext.setAttribute(SpanAttribute.WorkflowId, commands.workflowId)
    }
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    val errorLogger = new DamlContextualizedErrorLogger(
      logger = logger,
      loggingContext = loggingContext,
      correlationId = requestWithSubmissionId.commands.map(_.submissionId),
    )
    Timed.timedAndTrackedFuture(
      metrics.daml.commands.submissions,
      metrics.daml.commands.submissionsRunning,
      Timed
        .value(
          metrics.daml.commands.validation,
          validator.validate(
            requestWithSubmissionId,
            currentLedgerTime(),
            currentUtcTime(),
            maxDeduplicationDuration(),
          )(errorLogger),
        )
        .fold(
          t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
          service.submit(_).map(_ => Empty.defaultInstance),
        ),
    )
  }

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, executionContext)

  private def generateSubmissionIdIfEmpty(request: ApiSubmitRequest): ApiSubmitRequest =
    if (request.commands.exists(_.submissionId.isEmpty))
      request.update(_.commands.submissionId := submissionIdGenerator.generate())
    else
      request
}
