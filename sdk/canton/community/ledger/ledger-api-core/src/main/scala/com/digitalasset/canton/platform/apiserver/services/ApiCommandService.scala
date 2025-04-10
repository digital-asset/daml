// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.command_service.*
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService as CommandServiceGrpc
import com.daml.ledger.api.v2.commands.Commands
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.services.CommandService
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  SubmitAndWaitRequestValidator,
}
import com.digitalasset.canton.ledger.api.{ProxyCloseable, SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.ServerServiceDefinition

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class ApiCommandService(
    protected val service: CommandService & AutoCloseable,
    commandsValidator: CommandsValidator,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: Duration,
    generateSubmissionId: SubmissionIdGenerator,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CommandServiceGrpc
    with GrpcApiService
    with ProxyCloseable
    with NamedLogging {

  private[this] val validator = new SubmitAndWaitRequestValidator(commandsValidator)

  override def submitAndWait(request: SubmitAndWaitRequest): Future[SubmitAndWaitResponse] =
    enrichRequestAndSubmit(request = request)(service.submitAndWait)

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    enrichRequestAndSubmit(request = request)(service.submitAndWaitForTransaction)

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    enrichRequestAndSubmit(request = request)(
      service.submitAndWaitForTransactionTree
    )

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)

  private def enrichRequestAndSubmit[T](
      request: SubmitAndWaitRequest
  )(submit: SubmitAndWaitRequest => LoggingContextWithTrace => Future[T]): Future[T] = {
    val traceContext = getAnnotedCommandTraceContext(request.commands, telemetry)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(traceContext)
    val requestWithSubmissionId =
      request.update(_.optionalCommands.modify(generateSubmissionIdIfEmpty))
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationDuration,
      )(errorLoggingContext(requestWithSubmissionId))
      .fold(
        t =>
          Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        _ => submit(requestWithSubmissionId)(loggingContext),
      )
  }

  private def enrichRequestAndSubmit[T](
      request: SubmitAndWaitForTransactionRequest
  )(
      submit: SubmitAndWaitForTransactionRequest => LoggingContextWithTrace => Future[T]
  ): Future[T] = {
    val traceContext = getAnnotedCommandTraceContext(request.commands, telemetry)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(traceContext)
    val requestWithSubmissionId =
      request.update(_.optionalCommands.modify(generateSubmissionIdIfEmpty))
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationDuration,
      )(errorLoggingContext(requestWithSubmissionId))
      .fold(
        t =>
          Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        _ => submit(requestWithSubmissionId)(loggingContext),
      )
  }

  private def generateSubmissionIdIfEmpty(commands: Option[Commands]): Option[Commands] =
    if (commands.exists(_.submissionId.isEmpty)) {
      commands.map(_.copy(submissionId = generateSubmissionId.generate()))
    } else {
      commands
    }

  private def errorLoggingContext(request: SubmitAndWaitRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    ErrorLoggingContext.fromOption(logger, loggingContext, request.commands.map(_.submissionId))

  private def errorLoggingContext(request: SubmitAndWaitForTransactionRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    ErrorLoggingContext.fromOption(logger, loggingContext, request.commands.map(_.submissionId))
}
