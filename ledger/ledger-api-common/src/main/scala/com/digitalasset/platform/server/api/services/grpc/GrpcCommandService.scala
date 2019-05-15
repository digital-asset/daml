// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import com.digitalasset.daml.lf.data.Ref.LedgerId
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_service._
import com.digitalasset.ledger.api.validation.{CommandsValidator, SubmitAndWaitRequestValidator}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class GrpcCommandService(
    protected val service: CommandService with AutoCloseable,
    val ledgerId: LedgerId,
    identifierResolver: IdentifierResolver)
    extends CommandService
    with GrpcApiService
    with ProxyCloseable {

  protected val logger: Logger = LoggerFactory.getLogger(CommandService.getClass)

  private[this] val validator = new SubmitAndWaitRequestValidator(
    new CommandsValidator(ledgerId, identifierResolver))

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    validator.validate(request).fold(Future.failed, _ => service.submitAndWait(request))

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionIdResponse] =
    validator
      .validate(request)
      .fold(Future.failed, _ => service.submitAndWaitForTransactionId(request))

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionResponse] =
    validator
      .validate(request)
      .fold(Future.failed, _ => service.submitAndWaitForTransaction(request))

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionTreeResponse] =
    validator
      .validate(request)
      .fold(Future.failed, _ => service.submitAndWaitForTransactionTree(request))

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, DirectExecutionContext)

}
