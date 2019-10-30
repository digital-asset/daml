// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.auth.{AuthService, Authorizer}
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandCompletionService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class CommandCompletionServiceAuthorization(
    protected val service: GrpcCommandCompletionService with AutoCloseable,
    private val authorizer: Authorizer,
    private val authService: AuthService)
    extends CommandCompletionService
    with ProxyCloseable
    with GrpcApiService {

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    authorizer.requirePublicClaims(service.completionEnd)(request)

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse]): Unit =
    authorizer.requireClaimsForAllPartiesOnStream(request.parties, service.completionStream)(
      request,
      responseObserver)

  override def bindService(): ServerServiceDefinition =
    CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()

  def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, akka.NotUsed] =
    service.completionStreamSource(request)
}
