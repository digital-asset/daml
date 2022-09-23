// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import java.time.{Duration, Instant}

import com.daml.error.DamlContextualizedErrorLogger
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

import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandService(
    protected val service: CommandService with AutoCloseable,
    val ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: () => Option[Duration],
    generateSubmissionId: SubmissionIdGenerator,
    explicitDisclosureUnsafeEnabled: Boolean,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends CommandService
    with GrpcApiService
    with ProxyCloseable {

  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  private[this] val validator = new SubmitAndWaitRequestValidator(
    CommandsValidator(ledgerId, explicitDisclosureUnsafeEnabled)
  )

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    enrichRequestAndSubmit(request)(service.submitAndWait)

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    enrichRequestAndSubmit(request)(service.submitAndWaitForTransactionId)

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
  )(submit: SubmitAndWaitRequest => Future[T]): Future[T] = {
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationDuration(),
      )(contextualizedErrorLogger(requestWithSubmissionId))
      .fold(
        t => Future.failed(ValidationLogger.logFailure(requestWithSubmissionId, t)),
        _ => submit(requestWithSubmissionId),
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
      loggingContext: LoggingContext
  ) =
    new DamlContextualizedErrorLogger(logger, loggingContext, request.commands.map(_.submissionId))
}
