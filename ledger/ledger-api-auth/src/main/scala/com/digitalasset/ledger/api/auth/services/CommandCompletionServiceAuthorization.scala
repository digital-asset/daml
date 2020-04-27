// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class CommandCompletionServiceAuthorization(
    protected val service: CommandCompletionService with AutoCloseable,
    private val authorizer: Authorizer)
    extends CommandCompletionService
    with ProxyCloseable
    with GrpcApiService {

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    authorizer.requirePublicClaims(service.completionEnd)(request)

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse]): Unit =
    authorizer.requireReadClaimsForAllPartiesOnStream(request.parties, service.completionStream)(
      request,
      responseObserver)

  override def bindService(): ServerServiceDefinition =
    CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()

}
