// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

private[daml] final class CommandCompletionServiceAuthorization(
    protected val service: CommandCompletionService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends CommandCompletionService
    with ProxyCloseable
    with GrpcApiService {

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    authorizer.requirePublicClaims(service.completionEnd)(request)

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit =
    authorizer.requireReadClaimsForAllPartiesOnStreamWithApplicationId(
      parties = request.parties,
      applicationIdL = Lens.unit[CompletionStreamRequest].applicationId,
      call = service.completionStream,
    )(request, responseObserver)

  override def bindService(): ServerServiceDefinition =
    CommandCompletionServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

}
