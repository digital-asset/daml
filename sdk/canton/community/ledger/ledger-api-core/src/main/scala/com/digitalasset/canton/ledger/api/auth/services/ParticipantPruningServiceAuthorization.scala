// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningService
import com.daml.ledger.api.v2.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest,
  PruneResponse,
}
import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class ParticipantPruningServiceAuthorization(
    protected val service: ParticipantPruningService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends ParticipantPruningService
    with ProxyCloseable
    with GrpcApiService {

  override def bindService(): ServerServiceDefinition =
    ParticipantPruningServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

  override def prune(request: PruneRequest): Future[PruneResponse] =
    authorizer.requireAdminClaims(service.prune)(request)

}
