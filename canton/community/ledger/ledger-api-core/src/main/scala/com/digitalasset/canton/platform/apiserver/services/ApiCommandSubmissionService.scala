// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService as GrpcCommandSubmissionService
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest as ApiSubmitRequest,
}
import com.daml.metrics.Timed
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.services.CommandSubmissionService
import com.digitalasset.canton.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
import com.digitalasset.canton.ledger.api.{ProxyCloseable, SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.tracing.{Spanning, Traced}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class ApiCommandSubmissionService(
    override protected val service: CommandSubmissionService & AutoCloseable,
    commandsValidator: CommandsValidator,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: () => Option[Duration],
    submissionIdGenerator: SubmissionIdGenerator,
    metrics: Metrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, val tracer: Tracer)
    extends GrpcCommandSubmissionService
    with ProxyCloseable
    with GrpcApiService
    with Spanning
    with NamedLogging {

  private val validator = new SubmitRequestValidator(commandsValidator)

  override def submit(request: ApiSubmitRequest): Future[Empty] = {
    implicit val traceContext = getAnnotedCommandTraceContext(request.commands, telemetry)
    submitWithTraceContext(Traced(request))
  }

  def submitWithTraceContext(
      request: Traced[ApiSubmitRequest]
  ): Future[Empty] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(request.traceContext)
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request.value)
    val errorLogger: ContextualizedErrorLogger =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        requestWithSubmissionId.commands.map(_.submissionId),
      )
    Timed.timedAndTrackedFuture(
      metrics.daml.commands.submissions,
      metrics.daml.commands.submissionsRunning,
      Timed
        .value(
          metrics.daml.commands.validation,
          validator.validate(
            req = requestWithSubmissionId,
            currentLedgerTime = currentLedgerTime(),
            currentUtcTime = currentUtcTime(),
            maxDeduplicationDuration = maxDeduplicationDuration(),
            domainIdString = None,
          )(errorLogger),
        )
        .fold(
          t =>
            Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
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
