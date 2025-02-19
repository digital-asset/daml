// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.services.CommandCompletionServiceAuthorization.completionStreamClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import scalapb.lenses.Lens

import scala.concurrent.ExecutionContext

final class CommandCompletionServiceAuthorization(
    protected val service: CommandCompletionService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends CommandCompletionService
    with ProxyCloseable
    with GrpcApiService {

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit =
    authorizer.stream(service.completionStream)(completionStreamClaims(request)*)(
      request,
      responseObserver,
    )

  override def bindService(): ServerServiceDefinition =
    CommandCompletionServiceGrpc.bindService(this, executionContext)
}

object CommandCompletionServiceAuthorization {
  def completionStreamClaims(
      request: CompletionStreamRequest
  ): List[RequiredClaim[CompletionStreamRequest]] =
    RequiredClaim.MatchApplicationId(
      requestStringL = Lens.unit[CompletionStreamRequest].applicationId,
      skipApplicationIdValidationForAnyPartyReaders = true,
    ) :: request.parties.view.map(RequiredClaim.ReadAs[CompletionStreamRequest]).toList
}
