// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.error.ErrorCode.LoggedApiException
import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitReassignmentRequest,
  SubmitReassignmentResponse,
  SubmitRequest,
  SubmitResponse,
}
import com.daml.metrics.Timed
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.daml.tracing.{SpanAttribute, Telemetry, TelemetryContext}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.services.CommandSubmissionService
import com.digitalasset.canton.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
import com.digitalasset.canton.ledger.api.{SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.ledger.participant.state.v2.{ReassignmentCommand, WriteService}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.tracing.Traced

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final class ApiCommandSubmissionServiceV2(
    commandSubmissionService: CommandSubmissionService,
    commandsValidator: CommandsValidator,
    writeService: WriteService,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: () => Option[Duration],
    submissionIdGenerator: SubmissionIdGenerator,
    metrics: Metrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CommandSubmissionServiceGrpc.CommandSubmissionService
    with AutoCloseable
    with NamedLogging {
  import ApiConversions.*

  private val validator = new SubmitRequestValidator(commandsValidator)

  override def submit(request: SubmitRequest): Future[SubmitResponse] = {
    implicit val traceContext = getAnnotedCommandTraceContextV2(request.commands, telemetry)
    submitWithTraceContext(Traced(request))
  }

  def submitWithTraceContext(
      request: Traced[SubmitRequest]
  ): Future[SubmitResponse] = {
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
            req = toV1(requestWithSubmissionId),
            currentLedgerTime = currentLedgerTime(),
            currentUtcTime = currentUtcTime(),
            maxDeduplicationDuration = maxDeduplicationDuration(),
            domainIdString = requestWithSubmissionId.commands.map(_.domainId),
          )(errorLogger),
        )
        .fold(
          t =>
            Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
          commandSubmissionService.submit(_).map(_ => SubmitResponse()),
        ),
    )
  }

  override def submitReassignment(
      request: SubmitReassignmentRequest
  ): Future[SubmitReassignmentResponse] = {
    implicit val telemetryContext: TelemetryContext =
      telemetry.contextFromGrpcThreadLocalContext()
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    request.reassignmentCommand.foreach { command =>
      telemetryContext
        .setAttribute(SpanAttribute.ApplicationId, command.applicationId)
        .setAttribute(SpanAttribute.CommandId, command.commandId)
        .setAttribute(SpanAttribute.Submitter, command.submitter)
        .setAttribute(SpanAttribute.WorkflowId, command.workflowId)
    }
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    val errorLogger: ContextualizedErrorLogger =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        requestWithSubmissionId.reassignmentCommand.map(_.submissionId),
      )
    Timed
      .value(
        metrics.daml.commands.reassignmentValidation,
        validator.validateReassignment(request)(errorLogger),
      )
      .fold(
        t =>
          Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        request =>
          writeService
            .submitReassignment(
              submitter = request.submitter,
              applicationId = request.applicationId,
              commandId = request.commandId,
              submissionId = Some(request.submissionId),
              workflowId = request.workflowId,
              reassignmentCommand = request.reassignmentCommand match {
                case Left(assignCommand) =>
                  ReassignmentCommand.Assign(
                    sourceDomain = assignCommand.sourceDomainId,
                    targetDomain = assignCommand.targetDomainId,
                    unassignId = CantonTimestamp(assignCommand.unassignId),
                  )
                case Right(unassignCommand) =>
                  ReassignmentCommand.Unassign(
                    sourceDomain = unassignCommand.sourceDomainId,
                    targetDomain = unassignCommand.targetDomainId,
                    contractId = unassignCommand.contractId,
                  )
              },
            )
            .toScalaUnwrapped
            .transform(handleSubmissionResult)
            .andThen(logger.logErrorsOnCall[SubmitReassignmentResponse]),
      )
  }

  private def generateSubmissionIdIfEmpty(request: SubmitRequest): SubmitRequest =
    if (request.commands.exists(_.submissionId.isEmpty))
      request.update(_.commands.submissionId := submissionIdGenerator.generate())
    else
      request

  private def generateSubmissionIdIfEmpty(
      request: SubmitReassignmentRequest
  ): SubmitReassignmentRequest =
    if (request.reassignmentCommand.exists(_.submissionId.isEmpty))
      request.update(_.reassignmentCommand.submissionId := submissionIdGenerator.generate())
    else
      request

  private def handleSubmissionResult(result: Try[state.SubmissionResult])(implicit
      loggingContext: LoggingContextWithTrace
  ): Try[SubmitReassignmentResponse] = {
    import state.SubmissionResult.*
    result match {
      case Success(Acknowledged) =>
        logger.debug("Success")
        Success(SubmitReassignmentResponse())

      case Success(result: SynchronousError) =>
        logger.info(s"Rejected: ${result.description}")
        Failure(result.exception)

      // Do not log again on errors that are logging on creation
      case Failure(error: LoggedApiException) => Failure(error)
      case Failure(error) =>
        logger.info(s"Rejected: ${error.getMessage}")
        Failure(error)
    }
  }

  override def close(): Unit = ()
}
