// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
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
