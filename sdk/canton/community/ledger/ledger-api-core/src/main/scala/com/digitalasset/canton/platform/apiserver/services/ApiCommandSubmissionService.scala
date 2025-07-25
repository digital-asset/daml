// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitReassignmentRequest,
  SubmitReassignmentResponse,
  SubmitRequest,
  SubmitResponse,
}
import com.daml.ledger.api.v2.commands.Commands
import com.daml.metrics.Timed
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.daml.tracing.Telemetry
import com.digitalasset.base.error.ErrorCode.LoggedApiException
import com.digitalasset.canton.ledger.api.services.CommandSubmissionService
import com.digitalasset.canton.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
import com.digitalasset.canton.ledger.api.{SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.{ReassignmentCommand, SubmissionSyncService}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.TimerAndTrackOnShutdownSyntax
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcFUSExtended
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandProgressTracker,
  CommandResultHandle,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final class ApiCommandSubmissionService(
    commandSubmissionService: CommandSubmissionService & AutoCloseable,
    commandsValidator: CommandsValidator,
    submissionSyncService: SubmissionSyncService,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: Duration,
    submissionIdGenerator: SubmissionIdGenerator,
    tracker: CommandProgressTracker,
    metrics: LedgerApiServerMetrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CommandSubmissionServiceGrpc.CommandSubmissionService
    with AutoCloseable
    with NamedLogging {

  private val validator = new SubmitRequestValidator(commandsValidator)

  override def submit(request: SubmitRequest): Future[SubmitResponse] = {
    implicit val traceContext = getAnnotatedCommandTraceContext(request.commands, telemetry)
    submitWithTraceContext(Traced(request)).asGrpcResponse
  }

  def submitWithTraceContext(
      request: Traced[SubmitRequest]
  ): FutureUnlessShutdown[SubmitResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(request.traceContext)
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request.value)
    val errorLogger: ErrorLoggingContext =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        requestWithSubmissionId.commands.map(_.submissionId),
      )
    val resultHandle = requestWithSubmissionId.commands
      .map {
        case allCommands @ Commands(
              workflowId,
              userId,
              commandId,
              commands,
              deduplicationPeriod,
              minLedgerTimeAbs,
              minLedgerTimeRel,
              actAs,
              readAs,
              submissionId,
              disclosedContracts,
              synchronizerId,
              packageIdSelectionPreference,
              prefetchKeys,
            ) =>
          tracker.registerCommand(
            commandId,
            Option.when(submissionId.nonEmpty)(submissionId),
            userId,
            commands,
            actAs = allCommands.actAs.toSet,
          )(loggingContextWithTrace.traceContext)
      }
      .getOrElse(CommandResultHandle.NoOp)

    val result = Timed.timedAndTrackedFutureUS(
      metrics.commands.submissions,
      metrics.commands.submissionsRunning,
      Timed
        .value(
          metrics.commands.validation,
          validator.validate(
            req = requestWithSubmissionId,
            currentLedgerTime = currentLedgerTime(),
            currentUtcTime = currentUtcTime(),
            maxDeduplicationDuration = maxDeduplicationDuration,
          )(errorLogger),
        )
        .fold(
          t =>
            FutureUnlessShutdown
              .failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
          commandSubmissionService.submit(_).map(_ => SubmitResponse()),
        ),
    )
    resultHandle.extractFailure(result)
  }

  override def submitReassignment(
      request: SubmitReassignmentRequest
  ): Future[SubmitReassignmentResponse] = {
    implicit val traceContext: TraceContext =
      getAnnotatedReassignmentCommandTraceContext(request.reassignmentCommands, telemetry)
    submitReassignmentWithTraceContext(Traced(request)).asGrpcResponse
  }

  def submitReassignmentWithTraceContext(
      request: Traced[SubmitReassignmentRequest]
  ): FutureUnlessShutdown[SubmitReassignmentResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(request.traceContext)
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request.value)
    val errorLogger: ErrorLoggingContext =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        requestWithSubmissionId.reassignmentCommands.map(_.submissionId),
      )
    Timed
      .value(
        metrics.commands.reassignmentValidation,
        validator.validateReassignment(requestWithSubmissionId)(errorLogger),
      )
      .fold(
        t =>
          FutureUnlessShutdown
            .failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        request =>
          FutureUnlessShutdown.outcomeF(
            submissionSyncService
              .submitReassignment(
                submitter = request.submitter,
                userId = request.userId,
                commandId = request.commandId,
                submissionId = Some(request.submissionId),
                workflowId = request.workflowId,
                reassignmentCommands = request.reassignmentCommands.map {
                  case Left(assignCommand) =>
                    ReassignmentCommand.Assign(
                      sourceSynchronizer = assignCommand.sourceSynchronizerId,
                      targetSynchronizer = assignCommand.targetSynchronizerId,
                      reassignmentId = assignCommand.reassignmentId,
                    )
                  case Right(unassignCommand) =>
                    ReassignmentCommand.Unassign(
                      sourceSynchronizer = unassignCommand.sourceSynchronizerId,
                      targetSynchronizer = unassignCommand.targetSynchronizerId,
                      contractId = unassignCommand.contractId,
                    )
                },
              )
              .toScalaUnwrapped
              .transform(handleSubmissionResult)
              .thereafter(logger.logErrorsOnCall[SubmitReassignmentResponse])
          ),
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
    if (request.reassignmentCommands.exists(_.submissionId.isEmpty))
      request.update(_.reassignmentCommands.submissionId := submissionIdGenerator.generate())
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

  override def close(): Unit = commandSubmissionService.close()
}
