// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.error.{DamlContextualizedErrorLogger, ContextualizedErrorLogger}
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.validation.{CommandsValidator, SubmitAndWaitRequestValidator}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandService(
    protected val service: CommandService with AutoCloseable,
    val ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationTime: () => Option[Duration],
    generateSubmissionId: SubmissionIdGenerator,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends CommandService
    with GrpcApiService
    with ProxyCloseable {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private[this] val validator = new SubmitAndWaitRequestValidator(
    new CommandsValidator(ledgerId)
  )

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] = {
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationTime(),
      )
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        _ => service.submitAndWait(requestWithSubmissionId),
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
      )
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        _ => service.submitAndWaitForTransactionId(requestWithSubmissionId),
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
      )
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        _ => service.submitAndWaitForTransaction(requestWithSubmissionId),
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
      )
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        _ => service.submitAndWaitForTransactionTree(requestWithSubmissionId),
      )
  }

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)

  private def generateSubmissionIdIfEmpty(request: SubmitAndWaitRequest): SubmitAndWaitRequest = {
    if (request.commands.exists(_.submissionId.isEmpty)) {
      val commandsWithSubmissionId =
        request.commands.map(_.copy(submissionId = generateSubmissionId.generate()))
      request.copy(commands = commandsWithSubmissionId)
    } else {
      request
    }
  }
}
