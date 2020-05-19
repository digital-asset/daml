// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.daml.ledger.api.v1.ledger_configuration_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

final class LedgerConfigurationServiceAuthorization(
    protected val service: LedgerConfigurationService with AutoCloseable,
    private val authorizer: Authorizer)
    extends LedgerConfigurationService
    with ProxyCloseable
    with GrpcApiService {

  override def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse]): Unit =
    authorizer.requirePublicClaimsOnStream(service.getLedgerConfiguration)(
      request,
      responseObserver)

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
