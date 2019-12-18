// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.auth.Authorizer
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

final class ActiveContractsServiceAuthorization(
    protected val service: ActiveContractsService with AutoCloseable,
    private val authorizer: Authorizer)
    extends ActiveContractsService
    with ProxyCloseable
    with GrpcApiService {

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse]): Unit =
    authorizer.requireReadClaimsForTransactionFilterOnStream(
      request.filter,
      service.getActiveContracts)(request, responseObserver)

  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
