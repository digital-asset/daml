// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_service._
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class CommandServiceImpl(
    submitAndWaitResponse: Future[Empty],
    submitAndWaitForTransactionIdResponse: Future[SubmitAndWaitForTransactionIdResponse],
    submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
    submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse])
    extends CommandService {

  private var lastRequest: Option[SubmitAndWaitRequest] = None

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] = {
    this.lastRequest = Some(request)
    submitAndWaitResponse
  }

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionIdResponse] = {
    this.lastRequest = Some(request)
    submitAndWaitForTransactionIdResponse
  }

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionResponse] = {
    this.lastRequest = Some(request)
    submitAndWaitForTransactionResponse
  }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionTreeResponse] = {
    this.lastRequest = Some(request)
    submitAndWaitForTransactionTreeResponse
  }

  def getLastRequest: Option[SubmitAndWaitRequest] = this.lastRequest
}

object CommandServiceImpl {

  def createWithRef(
      submitAndWaitResponse: Future[Empty],
      submitAndWaitForTransactionIdResponse: Future[SubmitAndWaitForTransactionIdResponse],
      submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
      submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse])(
      implicit ec: ExecutionContext): (ServerServiceDefinition, CommandServiceImpl) = {
    val serviceImpl = new CommandServiceImpl(
      submitAndWaitResponse,
      submitAndWaitForTransactionIdResponse,
      submitAndWaitForTransactionResponse,
      submitAndWaitForTransactionTreeResponse)
    (CommandServiceGrpc.bindService(serviceImpl, ec), serviceImpl)
  }
}
