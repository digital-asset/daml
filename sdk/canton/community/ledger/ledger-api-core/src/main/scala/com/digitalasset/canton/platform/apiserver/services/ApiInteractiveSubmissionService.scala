// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest as SubmiteRequestP
import com.daml.ledger.api.v2.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService as InteractiveSubmissionServiceGrpc
import com.daml.ledger.api.v2.interactive_submission_service.{
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
import com.digitalasset.canton.tracing.Traced
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
    implicit val traceContext = getAnnotedCommandTraceContext(request.commands, telemetry)
    prepareWithTraceContext(Traced(request))
  }

  def prepareWithTraceContext(
      request: Traced[PrepareRequestP]
  ): Future[PrepareResponseP] = {
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
      metrics.commands.interactivePrepares,
      metrics.commands.preparesRunning,
      Timed
        .value(
          metrics.commands.validation,
          validator.validate(
            req = requestWithSubmissionId,
            currentLedgerTime = currentLedgerTime(),
            currentUtcTime = currentUtcTime(),
            maxDeduplicationDuration = maxDeduplicationDuration,
            domainIdString = requestWithSubmissionId.commands.flatMap(commands =>
              OptionUtil.emptyStringAsNone(commands.domainId)
            ),
          )(errorLogger),
        )
        .map { case SubmitRequest(commands) =>
          PrepareRequest(commands)
        }
        .fold(
          t =>
            Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
          interactiveSubmissionService.prepare(_),
        ),
    )
  }

  private def generateSubmissionIdIfEmpty(request: PrepareRequestP): SubmiteRequestP =
    if (request.commands.exists(_.submissionId.isEmpty))
      SubmiteRequestP(
        request.update(_.commands.submissionId := submissionIdGenerator.generate()).commands
      )
    else
      SubmiteRequestP(request.commands)

  override def close(): Unit = {}

  override def bindService(): ServerServiceDefinition =
    InteractiveSubmissionServiceGrpc.bindService(this, executionContext)
}
