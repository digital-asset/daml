// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.state_service.*
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateService
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.auth.services.StateServiceAuthorization.getActiveContractsClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import scalapb.lenses.Lens

import scala.annotation.nowarn
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
    authorizer.stream(service.getActiveContracts)(
      getActiveContractsClaims(request)*
    )(request, responseObserver)

  override def getConnectedSynchronizers(
      request: GetConnectedSynchronizersRequest
  ): Future[GetConnectedSynchronizersResponse] =
    authorizer.rpc(service.getConnectedSynchronizers)(
      RequiredClaim.AdminOrIdpAdminOrReadAsParty(request.party),
      RequiredClaim.MatchIdentityProviderId(
        Lens.unit[GetConnectedSynchronizersRequest].identityProviderId
      ),
    )(request)

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
    authorizer.rpc(service.getLedgerEnd)(RequiredClaim.Public())(request)

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] =
    authorizer.rpc(service.getLatestPrunedOffsets)(RequiredClaim.Public())(request)

  override def bindService(): ServerServiceDefinition =
    StateServiceGrpc.bindService(this, executionContext)
}

object StateServiceAuthorization {
  // TODO(#23504) remove checking filter when it is removed from GetActiveContractsRequest
  @nowarn("cat=deprecation")
  def getActiveContractsClaims(
      request: GetActiveContractsRequest
  ): List[RequiredClaim[GetActiveContractsRequest]] =
    request.eventFormat.toList.flatMap(
      RequiredClaims.eventFormatClaims[GetActiveContractsRequest]
    ) ::: request.filter.toList.flatMap(
      RequiredClaims.transactionFilterClaims[GetActiveContractsRequest]
    )
}
