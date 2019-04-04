// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_completion_service._
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

class CommandCompletionServiceImpl(
    completions: List[CompletionStreamResponse],
    end: CompletionEndResponse)
    extends CommandCompletionService {

  private var lastCompletionStreamRequest: Option[CompletionStreamRequest] = None
  private var lastCompletionEndRequest: Option[CompletionEndRequest] = None

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse]): Unit = {
    this.lastCompletionStreamRequest = Some(request)
    completions.foreach(responseObserver.onNext)
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] = {
    this.lastCompletionEndRequest = Some(request)
    Future.successful(end)
  }

  def getLastCompletionStreamRequest: Option[CompletionStreamRequest] =
    this.lastCompletionStreamRequest
  def getLastCompletionEndRequest: Option[CompletionEndRequest] = this.lastCompletionEndRequest
}

object CommandCompletionServiceImpl {

  def createWithRef(completions: List[CompletionStreamResponse], end: CompletionEndResponse)(
      implicit ec: ExecutionContext): (ServerServiceDefinition, CommandCompletionServiceImpl) = {
    val serviceImpl = new CommandCompletionServiceImpl(completions, end)
    (CommandCompletionServiceGrpc.bindService(serviceImpl, ec), serviceImpl)
  }
}
