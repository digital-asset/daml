// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_service._
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class CommandServiceImpl(
                          submitAndWaitResponse: Future[SubmitAndWaitResponse],
                          submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
                          submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse]) extends CommandService {

  private var lastRequest: Option[SubmitAndWaitRequest] = None

  override def submitAndWait(request: SubmitAndWaitRequest): Future[SubmitAndWaitResponse] = {
    this.lastRequest = Some(request)
    submitAndWaitResponse
  }


  override def submitAndWaitForTransaction(request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionResponse] = {
    this.lastRequest = Some(request)
    submitAndWaitForTransactionResponse
  }

  override def submitAndWaitForTransactionTree(request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionTreeResponse] = {
    this.lastRequest = Some(request)
    submitAndWaitForTransactionTreeResponse
  }

  def getLastRequest: Option[SubmitAndWaitRequest] = this.lastRequest
}

object CommandServiceImpl {

  def createWithRef(submitAndWaitResponse: Future[SubmitAndWaitResponse],
                    submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
                    submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse])(
      implicit ec: ExecutionContext): (ServerServiceDefinition, CommandServiceImpl) = {
    val serviceImpl = new CommandServiceImpl(submitAndWaitResponse, submitAndWaitForTransactionResponse, submitAndWaitForTransactionTreeResponse)
    (CommandServiceGrpc.bindService(serviceImpl, ec), serviceImpl)
  }
}
