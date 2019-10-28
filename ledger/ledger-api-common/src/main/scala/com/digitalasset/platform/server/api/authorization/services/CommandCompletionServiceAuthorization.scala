// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization.services

import akka.stream.scaladsl.Source
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.authorization.ApiServiceAuthorization
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandCompletionService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class CommandCompletionServiceAuthorization(
    protected val service: GrpcCommandCompletionService with AutoCloseable,
    protected val authService: AuthService)
    extends CommandCompletionService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(CommandCompletionService.getClass)

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    ApiServiceAuthorization
      .requirePublicClaims()
      .fold(Future.failed(_), _ => service.completionEnd(request))

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse]): Unit =
    ApiServiceAuthorization
      .requireClaimsForAllParties(request.parties.toSet)
      .fold(responseObserver.onError, _ => service.completionStream(request, responseObserver))

  override def bindService(): ServerServiceDefinition =
    CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()

  def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, akka.NotUsed] =
    service.completionStreamSource(request)
}
