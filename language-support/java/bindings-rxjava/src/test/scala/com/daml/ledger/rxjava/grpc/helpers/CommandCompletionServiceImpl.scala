// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.CommandCompletionServiceV2Authorization
import com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v2.command_completion_service._
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext

final class CommandCompletionServiceImpl(
    completions: List[CompletionStreamResponse]
) extends CommandCompletionService
    with FakeAutoCloseable {

  private var lastCompletionStreamRequest: Option[CompletionStreamRequest] = None

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit = {
    this.lastCompletionStreamRequest = Some(request)
    completions.foreach(responseObserver.onNext)
  }

  def getLastCompletionStreamRequest: Option[CompletionStreamRequest] =
    this.lastCompletionStreamRequest
}

object CommandCompletionServiceImpl {
  def createWithRef(
      completions: List[CompletionStreamResponse],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, CommandCompletionServiceImpl) = {
    val impl = new CommandCompletionServiceImpl(completions)
    val authImpl = new CommandCompletionServiceV2Authorization(impl, authorizer)
    (CommandCompletionServiceGrpc.bindService(authImpl, ec), impl)
  }
}
