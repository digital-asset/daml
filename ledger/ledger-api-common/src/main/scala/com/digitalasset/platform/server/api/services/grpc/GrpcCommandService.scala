// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.validation.SubmitAndWaitRequestValidator
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.services.domain
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import com.google.protobuf.empty.Empty
import io.grpc.{Context, ServerServiceDefinition}

import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandService(
    protected val service: domain.CommandService with AutoCloseable,
    val ledgerId: LedgerId,
    generateSubmissionId: SubmissionIdGenerator,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationTime: () => Option[Duration],
    validator: SubmitAndWaitRequestValidator,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends CommandService
    with GrpcApiService
    with ProxyCloseable {

  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] = {
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationTime(),
      )(contextualizedErrorLogger(requestWithSubmissionId))
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        request =>
          service.submitAndWait(request, timeout()).map { _ =>
            Empty.defaultInstance
          },
      )
  }

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] = {
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationTime(),
      )(contextualizedErrorLogger(requestWithSubmissionId))
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        request => service.submitAndWait(request, timeout()),
      )
  }

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] = {
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationTime(),
      )(contextualizedErrorLogger(requestWithSubmissionId))
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        request => service.submitAndWaitForTransaction(request, timeout()),
      )
  }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] = {
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationTime(),
      )(contextualizedErrorLogger(requestWithSubmissionId))
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        request => service.submitAndWaitForTransactionTree(request, timeout()),
      )
  }

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)

  private def generateSubmissionIdIfEmpty(request: SubmitAndWaitRequest): SubmitAndWaitRequest =
    if (request.commands.exists(_.submissionId.isEmpty)) {
      val commandsWithSubmissionId =
        request.commands.map(_.copy(submissionId = generateSubmissionId.generate()))
      request.copy(commands = commandsWithSubmissionId)
    } else {
      request
    }

  private def contextualizedErrorLogger(request: SubmitAndWaitRequest)(implicit
      loggingContext: LoggingContext
  ) =
    new DamlContextualizedErrorLogger(logger, loggingContext, request.commands.map(_.submissionId))

  private def timeout() = Option(Context.current().getDeadline)
    .map(deadline => Duration.ofNanos(deadline.timeRemaining(TimeUnit.NANOSECONDS)))
}
