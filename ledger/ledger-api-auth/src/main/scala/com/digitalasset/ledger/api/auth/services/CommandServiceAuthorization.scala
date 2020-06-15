// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future

/** Note: the command service internally uses calls to the CommandSubmissionService and CommandCompletionService.
  * These calls already require authentication, but it is better to check authorization here as well.
  */
final class CommandServiceAuthorization(
    protected val service: CommandService with AutoCloseable,
    private val authorizer: Authorizer)
    extends CommandService
    with ProxyCloseable
    with GrpcApiService {

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    authorizer.requireActClaimsForParty(
      party = request.commands.map(_.party),
      applicationId = request.commands.map(_.applicationId),
      call = service.submitAndWait,
    )(request)

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionResponse] =
    authorizer.requireActClaimsForParty(
      party = request.commands.map(_.party),
      applicationId = request.commands.map(_.applicationId),
      call = service.submitAndWaitForTransaction,
    )(request)

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionIdResponse] =
    authorizer.requireActClaimsForParty(
      party = request.commands.map(_.party),
      applicationId = request.commands.map(_.applicationId),
      call = service.submitAndWaitForTransactionId,
    )(request)

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionTreeResponse] =
    authorizer.requireActClaimsForParty(
      party = request.commands.map(_.party),
      applicationId = request.commands.map(_.applicationId),
      call = service.submitAndWaitForTransactionTree,
    )(request)

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()

}
