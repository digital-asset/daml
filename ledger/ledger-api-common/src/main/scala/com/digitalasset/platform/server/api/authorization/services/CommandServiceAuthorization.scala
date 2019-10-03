// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization.services

import com.daml.ledger.participant.state.v1.AuthService
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest
}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.authorization.ApiServiceAuthorization
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

/** Note: the command service internally uses calls to the CommandSubmissionService and CommandCompletionService.
  * These calls already require authentication, but it is better to check authorization here as well.
  */
class CommandServiceAuthorization(
    protected val service: CommandService with AutoCloseable,
    protected val authService: AuthService)
    extends CommandService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(CommandService.getClass)

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    ApiServiceAuthorization
      .requireClaimsForParty(request.commands.map(_.party))
      .fold(Future.failed(_), _ => service.submitAndWait(request))

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionResponse] =
    ApiServiceAuthorization
      .requireClaimsForParty(request.commands.map(_.party))
      .fold(Future.failed(_), _ => service.submitAndWaitForTransaction(request))

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionIdResponse] =
    ApiServiceAuthorization
      .requireClaimsForParty(request.commands.map(_.party))
      .fold(Future.failed(_), _ => service.submitAndWaitForTransactionId(request))

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionTreeResponse] =
    ApiServiceAuthorization
      .requireClaimsForParty(request.commands.map(_.party))
      .fold(Future.failed(_), _ => service.submitAndWaitForTransactionTree(request))

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()

}
