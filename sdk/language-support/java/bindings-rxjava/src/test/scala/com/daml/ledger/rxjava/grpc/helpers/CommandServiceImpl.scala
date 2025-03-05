// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.CommandServiceAuthorization
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v2.command_service._
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class CommandServiceImpl(
    submitAndWaitResponse: Future[SubmitAndWaitResponse],
    submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
    submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
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

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] = {
    this.lastCommands = Some(request.getCommands)
    submitAndWaitForTransactionTreeResponse
  }

  def getLastCommands: Option[Commands] = this.lastCommands
}

object CommandServiceImpl {

  def createWithRef(
      submitAndWaitResponse: Future[SubmitAndWaitResponse],
      submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
      submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, CommandServiceImpl) = {
    val impl = new CommandServiceImpl(
      submitAndWaitResponse,
      submitAndWaitForTransactionResponse,
      submitAndWaitForTransactionTreeResponse,
    )
    val authImpl = new CommandServiceAuthorization(impl, authorizer)
    (CommandServiceGrpc.bindService(authImpl, ec), impl)
  }
}
