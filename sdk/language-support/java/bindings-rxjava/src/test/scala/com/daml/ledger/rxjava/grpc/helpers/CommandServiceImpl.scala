// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.CommandServiceAuthorization
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v2.command_service._
import io.grpc.ServerServiceDefinition

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

final class CommandServiceImpl(
    submitAndWaitResponse: Future[SubmitAndWaitResponse],
    submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
    @deprecated("Use submitAndWaitForTransactionResponse instead", "3.4.0")
    submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
    submitAndWaitForReassignmentResponse: Future[SubmitAndWaitForReassignmentResponse],
) extends CommandService
    with FakeAutoCloseable {

  private var lastCommands: Option[Commands] = None

  override def submitAndWait(request: SubmitAndWaitRequest): Future[SubmitAndWaitResponse] = {
    this.lastCommands = Some(request.getCommands)
    submitAndWaitResponse
  }

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  ): Future[SubmitAndWaitForTransactionResponse] = {
    this.lastCommands = Some(request.getCommands)
    submitAndWaitForTransactionResponse
  }

  @deprecated("Use submitAndWaitForTransaction instead", "3.4.0")
  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] = {
    this.lastCommands = Some(request.getCommands)
    submitAndWaitForTransactionTreeResponse
  }

  override def submitAndWaitForReassignment(
      request: SubmitAndWaitForReassignmentRequest
  ): Future[SubmitAndWaitForReassignmentResponse] = {
    submitAndWaitForReassignmentResponse
  }

  def getLastCommands: Option[Commands] = this.lastCommands
}

object CommandServiceImpl {

  // TODO remove submitAndWaitForTransactionTreeResponse
  @nowarn("cat=deprecation")
  def createWithRef(
      submitAndWaitResponse: Future[SubmitAndWaitResponse],
      submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
      submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
      submitAndWaitForReassignmentResponse: Future[SubmitAndWaitForReassignmentResponse],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, CommandServiceImpl) = {
    val impl = new CommandServiceImpl(
      submitAndWaitResponse,
      submitAndWaitForTransactionResponse,
      submitAndWaitForTransactionTreeResponse,
      submitAndWaitForReassignmentResponse,
    )
    val authImpl = new CommandServiceAuthorization(impl, authorizer)
    (CommandServiceGrpc.bindService(authImpl, ec), impl)
  }
}
