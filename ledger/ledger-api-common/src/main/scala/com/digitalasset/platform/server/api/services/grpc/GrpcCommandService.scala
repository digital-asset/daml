// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import java.time.{Duration, Instant}

import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.validation.{CommandsValidator, SubmitAndWaitRequestValidator}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandService(
    protected val service: CommandService with AutoCloseable,
    val ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationTime: () => Option[Duration],
    minSkew: () => Option[Duration],
    generateSubmissionId: SubmissionIdGenerator,
)(implicit executionContext: ExecutionContext)
    extends CommandService
    with GrpcApiService
    with ProxyCloseable {

  protected implicit val logger: Logger = LoggerFactory.getLogger(service.getClass)

  private[this] val validator =
    new SubmitAndWaitRequestValidator(new CommandsValidator(ledgerId, generateSubmissionId))

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    validator
      .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime(), minSkew())
      .fold(
        t => Future.failed(ValidationLogger.logFailure(request, t)),
        _ => service.submitAndWait(request),
      )

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    validator
      .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime(), minSkew())
      .fold(
        t => Future.failed(ValidationLogger.logFailure(request, t)),
        _ => service.submitAndWaitForTransactionId(request),
      )

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    validator
      .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime(), minSkew())
      .fold(
        t => Future.failed(ValidationLogger.logFailure(request, t)),
        _ => service.submitAndWaitForTransaction(request),
      )

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    validator
      .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime(), minSkew())
      .fold(
        t => Future.failed(ValidationLogger.logFailure(request, t)),
        _ => service.submitAndWaitForTransactionTree(request),
      )

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)

}
