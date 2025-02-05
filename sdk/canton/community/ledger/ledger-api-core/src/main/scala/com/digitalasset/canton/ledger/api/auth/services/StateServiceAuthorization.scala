// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.state_service.*
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateService
import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

final class StateServiceAuthorization(
    protected val service: StateService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends StateService
    with ProxyCloseable
    with GrpcApiService {

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse],
  ): Unit =
    authorizer.requireReadClaimsForTransactionFilterOnStream(
      request.filter
        .map(_.filtersByParty)
        .orElse(request.eventFormat.map(_.filtersByParty)),
      request.filter
        .flatMap(_.filtersForAnyParty)
        .orElse(request.eventFormat.flatMap(_.filtersForAnyParty))
        .nonEmpty,
      service.getActiveContracts,
    )(request, responseObserver)

  override def getConnectedSynchronizers(
      request: GetConnectedSynchronizersRequest
  ): Future[GetConnectedSynchronizersResponse] =
    authorizer.requireReadClaimsForAllParties(
      List(request.party),
      service.getConnectedSynchronizers,
    )(request)

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
    authorizer.requirePublicClaims(service.getLedgerEnd)(request)

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] =
    authorizer.requirePublicClaims(service.getLatestPrunedOffsets)(request)

  override def bindService(): ServerServiceDefinition =
    StateServiceGrpc.bindService(this, executionContext)
}
