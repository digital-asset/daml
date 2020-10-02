// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import java.time.{Duration, Instant}

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.validation.{CommandsValidator, SubmitAndWaitRequestValidator}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.dec.DirectExecutionContext
import com.daml.platform.server.api.ProxyCloseable
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class GrpcCommandService(
    protected val service: CommandService with AutoCloseable,
    val ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationTime: () => Option[Duration]
) extends CommandService
    with GrpcApiService
    with ProxyCloseable {

  protected val logger: Logger = LoggerFactory.getLogger(CommandService.getClass)

  private[this] val validator =
    new SubmitAndWaitRequestValidator(new CommandsValidator(ledgerId))

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    validator
      .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime())
      .fold(Future.failed, _ => service.submitAndWait(request))

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionIdResponse] =
    validator
      .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime())
      .fold(Future.failed, _ => service.submitAndWaitForTransactionId(request))

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionResponse] =
    validator
      .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime())
      .fold(Future.failed, _ => service.submitAndWaitForTransaction(request))

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionTreeResponse] =
    validator
      .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime())
      .fold(Future.failed, _ => service.submitAndWaitForTransactionTree(request))

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, DirectExecutionContext)

}
