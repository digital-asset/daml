// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService as InteractiveSubmissionServiceGrpc
import com.daml.ledger.api.v2.interactive_submission_service.{
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  PrepareSubmissionRequest as PrepareRequestP,
  PrepareSubmissionResponse as PrepareResponseP,
}
import com.daml.metrics.Timed
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.PrepareRequest
import com.digitalasset.canton.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
import com.digitalasset.canton.ledger.api.{SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.OptionUtil
import io.grpc.ServerServiceDefinition

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class ApiInteractiveSubmissionService(
    interactiveSubmissionService: InteractiveSubmissionService & AutoCloseable,
    commandsValidator: CommandsValidator,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: Duration,
    submissionIdGenerator: SubmissionIdGenerator,
    metrics: LedgerApiServerMetrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends InteractiveSubmissionServiceGrpc
    with GrpcApiService
    with NamedLogging {

  private val validator = new SubmitRequestValidator(commandsValidator)

  override def prepareSubmission(request: PrepareRequestP): Future[PrepareResponseP] = {
    implicit val traceContext = getPrepareRequestTraceContext(
      request.applicationId,
      request.commandId,
      request.actAs,
      telemetry,
    )
    prepareWithTraceContext(Traced(request))
  }

  def prepareWithTraceContext(
      request: Traced[PrepareRequestP]
  ): Future[PrepareResponseP] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(request.traceContext)
    val errorLogger: ContextualizedErrorLogger =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        None,
      )

    Timed.timedAndTrackedFuture(
      metrics.commands.interactivePrepares,
      metrics.commands.preparesRunning,
      Timed
        .value(
          metrics.commands.validation,
          validator.validatePrepare(
            req = request.value,
            currentLedgerTime = currentLedgerTime(),
            currentUtcTime = currentUtcTime(),
            maxDeduplicationDuration = maxDeduplicationDuration,
          )(errorLogger),
        )
        .map { case SubmitRequest(commands) =>
          PrepareRequest(commands)
        }
        .fold(
          t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          interactiveSubmissionService.prepare(_),
        ),
    )
  }

  override def executeSubmission(
      request: ExecuteSubmissionRequest
  ): Future[ExecuteSubmissionResponse] = {
    // TODO(i20660) Extract command id and actAs from serialized transaction once serialization is in place
    // and get a better trace context
    val traceContext =
      TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(traceContext)
    val errorLogger: ContextualizedErrorLogger =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        OptionUtil.emptyStringAsNone(request.submissionId),
      )
    validator
      .validateExecute(
        request,
        submissionIdGenerator,
        maxDeduplicationDuration,
      )(errorLogger)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        interactiveSubmissionService.execute(_),
      )
  }

  override def close(): Unit = {}

  override def bindService(): ServerServiceDefinition =
    InteractiveSubmissionServiceGrpc.bindService(this, executionContext)
}
