// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService as CommandServiceGrpc
import com.daml.ledger.api.v2.command_service.*
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
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class ApiCommandService(
    protected val service: CommandService & AutoCloseable,
    commandsValidator: CommandsValidator,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: () => Option[Duration],
    generateSubmissionId: SubmissionIdGenerator,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CommandServiceGrpc
    with GrpcApiService
    with ProxyCloseable
    with NamedLogging {

  private[this] val validator = new SubmitAndWaitRequestValidator(commandsValidator)

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    enrichRequestAndSubmit(request)(service.submitAndWait)

  override def submitAndWaitForUpdateId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForUpdateIdResponse] =
    enrichRequestAndSubmit(request)(service.submitAndWaitForUpdateId)

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    enrichRequestAndSubmit(request)(service.submitAndWaitForTransaction)

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    enrichRequestAndSubmit(request)(service.submitAndWaitForTransactionTree)

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)

  private def enrichRequestAndSubmit[T](
      request: SubmitAndWaitRequest
  )(submit: SubmitAndWaitRequest => LoggingContextWithTrace => Future[T]): Future[T] = {
    val traceContext = getAnnotedCommandTraceContextV2(request.commands, telemetry)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(traceContext)
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        ApiConversions.toV1(
          requestWithSubmissionId
        ), // it is enough to validate V1 only, since at submission the SubmitRequest will be validated again (and the domainId as well)
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationDuration(),
      )(contextualizedErrorLogger(requestWithSubmissionId))
      .fold(
        t =>
          Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        _ => submit(requestWithSubmissionId)(loggingContext),
      )
  }

  private def generateSubmissionIdIfEmpty(request: SubmitAndWaitRequest): SubmitAndWaitRequest =
    if (request.commands.exists(_.submissionId.isEmpty)) {
      val commandsWithSubmissionId =
        request.commands.map(_.copy(submissionId = generateSubmissionId.generate()))
      request.copy(commands = commandsWithSubmissionId)
    } else {
      request
    }

  private def contextualizedErrorLogger(request: SubmitAndWaitRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    ErrorLoggingContext.fromOption(logger, loggingContext, request.commands.map(_.submissionId))
}
