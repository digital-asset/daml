// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v1.command_completion_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class CommandCompletionServiceAuthorization(
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
